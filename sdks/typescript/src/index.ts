/**
 * The WasmKeep TypeScript SDK: write actors as plain TypeScript.
 *
 * ```ts
 * import { defineActor, storage } from "@wasmkeep/sdk";
 *
 * export default defineActor({
 *   init() {
 *     storage.exec("CREATE TABLE IF NOT EXISTS counter (n INTEGER NOT NULL)");
 *   },
 *   fetch(req) {
 *     return `hello from ${req.path}\n`;
 *   },
 * });
 * ```
 *
 * Build with `wasmkeep-ts build src/index.ts -o actor.wasm`, then serve with
 * `wasmkeep serve --actor NAME=actor.wasm`. Every handler runs one event at a
 * time per actor: no locks needed.
 */

/// <reference path="./globals.d.ts" />
// The host interfaces imported below (`wasmkeep:actor/*`, `wasi:*`): this
// reference is how a project that installed the package finds their types.
/// <reference path="../types/actor.d.ts" />
// First: installs TextEncoder/TextDecoder on engines without them (QuickJS).
import "./text";
import "./console";
import { explainMissing } from "./engines";
import { describeError } from "./errors";
import * as wkStorage from "wasmkeep:actor/storage@0.2.0";
import * as wkAlarms from "wasmkeep:actor/alarms@0.2.0";
import * as wkSockets from "wasmkeep:actor/websockets@0.2.0";
import type { Value } from "wasmkeep:actor/types@0.2.0";
import type { IncomingRequest, ResponseOutparam } from "wasi:http/types@0.2.12";
import { release, respond, toRequest } from "./handler";
import type { ActorRequest, ActorResponse } from "./request";

/** A database kept by one actor (`db.define({ migrations })`,
 * `new db.Client("db/main")`); see ./db.ts. */
export * as db from "./db";
export * as directory from "./directory";
export * as broadcast from "./broadcast";
/** Key-value storage over shard actors (`new kv.Client("kv", 8)`); see ./kv.ts. */
export * as kv from "./kv";
/** Queues over queue actors (`new queue.Client("queue/jobs")`,
 * `queue.handleDelivery`); see ./queue.ts. */
export * as queue from "./queue";
/** Durable workflows on actors (`workflow.define(...)`,
 * `new workflow.Client("orders")`); see ./workflow.ts. */
export * as workflow from "./workflow";
/** Synchronous outbound HTTP (`http.fetch(url, {...})`); see ./http.ts. */
export * as http from "./http";

// ---------------------------------------------------------------- storage

/** A value bound to or read from SQL. Integers that do not fit a JS number
 * exactly are `bigint`. */
export type SqlValue = null | number | bigint | string | Uint8Array | boolean;

/** One row, by column name. */
export type Row = Record<string, SqlValue>;

/** A failed SQL statement. `kind` is one of: syntax, constraint, bind,
 * multiple-statements, quota-exceeded, not-allowed, internal. */
export class SqlError extends Error {
  constructor(
    readonly kind: string,
    message: string,
  ) {
    super(message);
    this.name = "SqlError";
  }
}

function toValue(v: SqlValue): Value {
  if (v === null || v === undefined) return { tag: "null" };
  if (typeof v === "boolean") return { tag: "integer", val: v ? 1n : 0n };
  if (typeof v === "bigint") return { tag: "integer", val: v };
  if (typeof v === "number") {
    return Number.isInteger(v) ? { tag: "integer", val: BigInt(v) } : { tag: "real", val: v };
  }
  if (typeof v === "string") return { tag: "text", val: v };
  return { tag: "blob", val: v };
}

function fromValue(v: Value): SqlValue {
  switch (v.tag) {
    case "null":
      return null;
    case "integer":
      return v.val >= BigInt(Number.MIN_SAFE_INTEGER) && v.val <= BigInt(Number.MAX_SAFE_INTEGER)
        ? Number(v.val)
        : v.val;
    case "real":
      return v.val;
    case "text":
      return v.val;
    case "blob":
      return v.val;
  }
}

function sql<T>(f: () => T): T {
  try {
    return f();
  } catch (e) {
    // jco throws WIT errors as an object whose `payload` is the error value.
    const p = (e as { payload?: { kind?: string; message?: string } }).payload;
    if (p && typeof p.message === "string") throw new SqlError(p.kind ?? "internal", p.message);
    throw e;
  }
}

/** The actor's private SQLite database. */
export const storage = {
  /** Runs a statement that returns no rows; returns the rows changed. */
  exec(statement: string, ...params: SqlValue[]): number {
    return Number(sql(() => wkStorage.exec(statement, params.map(toValue))));
  },
  /** Like `exec`, but returns only once the write is durable (replicated to
   * the cluster's bucket, or fsynced on a single node). Inside a transaction
   * it takes only `COMMIT` (or `END`), which makes the whole transaction
   * durable. */
  execDurable(statement: string, ...params: SqlValue[]): number {
    return Number(sql(() => wkStorage.execDurable(statement, params.map(toValue))));
  },
  /** Runs a statement and returns its rows as objects. */
  query(statement: string, ...params: SqlValue[]): Row[] {
    const rows = sql(() => wkStorage.query(statement, params.map(toValue)));
    return rows.values.map((values) => {
      const row: Row = {};
      rows.columns.forEach((c, i) => (row[c] = fromValue(values[i])));
      return row;
    });
  },
  /** Deletes this actor once the current event ends. */
  deleteSelf(): void {
    wkStorage.deleteSelf();
  },
  /** Deletes this actor now, and returns only once the delete is durable
   * (recorded in the cluster's bucket, or the files removed and fsynced on
   * a single node). Later storage calls in the same event throw
   * `not-allowed`. Throws a `SqlError`: `not-allowed` inside a transaction
   * (nothing changed), `internal` when the delete is not known to be
   * durable (calling it again is safe). */
  deleteSelfDurable(): void {
    sql(() => wkStorage.deleteSelfDurable());
  },
  /** Returns once every write this actor committed before the call is
   * durable (in the cluster's bucket, or fsynced on a single node), without
   * running a statement. Throws a `SqlError`: `not-allowed` inside a
   * transaction, `internal` when those writes are not known to be durable
   * (fail the request; a message starting `not durable: still uploading`
   * means the writes stay committed and usually become durable soon after).
   * Needs a node of the same release: a component built with this SDK
   * imports it even when it never calls it. */
  flush(): void {
    sql(() => wkStorage.flush());
  },
};

// ----------------------------------------------------------------- alarms

/** The actor's single alarm. It fires `alarm()` at least once, also after a
 * crash or a move to another node. */
export const alarms = {
  /** Sets (replaces) the alarm: a time in ms since the Unix epoch, or a Date. */
  set(at: number | Date): void {
    wkAlarms.setAlarm(BigInt(at instanceof Date ? at.getTime() : Math.floor(at)));
  },
  /** The pending alarm (ms since the Unix epoch), if any. */
  get(): number | undefined {
    // An unset alarm (WIT `none`) is `undefined` on StarlingMonkey, `null` on QuickJS.
    const at: bigint | null | undefined = wkAlarms.getAlarm();
    return at == null ? undefined : Number(at);
  },
  /** Cancels the alarm. */
  clear(): void {
    wkAlarms.clearAlarm();
  },
};

// ----------------------------------------------------------- actor calls

export { actors, env, CallError, type ActorAddress, type CallRequest, type CallResponse } from "./actors";
/** Who sent a request, its host and scheme, and the answer to a failed
 * call (ADR 0023); see ./request.ts. */
export {
  caller,
  host,
  json,
  HeaderList,
  parseCaller,
  scheme,
  unavailable,
  type ActorRequest,
  type ActorResponse,
  type Caller,
  type HeadersInit,
} from "./request";

/** Typed operation contracts, codecs, the structured error envelope and
 * the typed client (`actorClient()`); see ./pure.ts and docs/typed-clients.md. */
export * from "./pure";
export { actorClient, actorTransport } from "./transport";

// ------------------------------------------------------------- websockets

/** A WebSocket connection id. */
export type ConnId = bigint;

import { trySend } from "./sockets";
export const sockets = {
  trySend,
  /** Sends a text or binary message. Returns false if the connection is
   * gone or its buffer is full. */
  send(conn: ConnId, data: string | Uint8Array): boolean {
    try {
      wkSockets.send(conn, typeof data === "string" ? { tag: "text", val: data } : { tag: "binary", val: data });
      return true;
    } catch {
      return false;
    }
  },
  /** Sends to every open connection of this actor, except `except`. */
  broadcast(data: string | Uint8Array, except?: ConnId): void {
    for (const conn of wkSockets.connections()) {
      if (conn !== except) sockets.send(conn, data);
    }
  },
  /** Closes a connection. */
  close(conn: ConnId, code = 1000, reason = ""): void {
    wkSockets.close(conn, code, reason);
  },
  /** This actor's open connections. */
  connections(): ConnId[] {
    return Array.from(wkSockets.connections());
  },
};

// ------------------------------------------------------------------- HTTP

/** An actor: every hook is optional. */
export interface ActorDefinition {
  /** Runs once per component version, before any other event (migrations). */
  init?(): void;
  /** Handles an HTTP request. */
  fetch?(req: ActorRequest): ActorResponse;
  /** The alarm fired. Throwing retries it with backoff. */
  alarm?(): void;
  websocket?: {
    /** A client connected. Return false (or throw) to refuse it. */
    open?(conn: ConnId, path: string): boolean | void;
    message?(conn: ConnId, data: string | Uint8Array): void;
    close?(conn: ConnId, code: number, reason: string): void;
  };
}

/** Declares an actor; `export default` it from the entry module. */
export function defineActor(definition: ActorDefinition): ActorDefinition {
  return definition;
}

/** The component exports for `definition`. Called by the build's generated
 * entry module; not needed in actor code. */
export function exportsFor(definition: ActorDefinition) {
  const failed = (e: unknown) => explainMissing(describeError(e));
  return {
    // A thrown string is the WIT `result` error (shown in the host's logs).
    onInit(): void {
      try {
        definition.init?.();
      } catch (e) {
        throw failed(e);
      }
    },
    onAlarm(): void {
      try {
        definition.alarm?.();
      } catch (e) {
        throw failed(e);
      }
    },
    onWebsocketOpen(conn: bigint, path: string): void {
      let accepted: boolean | void = true;
      try {
        accepted = definition.websocket?.open ? definition.websocket.open(conn, path) : true;
      } catch (e) {
        throw failed(e);
      }
      if (accepted === false) throw "refused";
    },
    onWebsocketMessage(conn: bigint, msg: { tag: "text"; val: string } | { tag: "binary"; val: Uint8Array }): void {
      try {
        definition.websocket?.message?.(conn, msg.val);
      } catch (e) {
        throw Error(failed(e));
      }
    },
    onWebsocketClose(conn: bigint, code: number, reason: string): void {
      try {
        definition.websocket?.close?.(conn, code, reason);
      } catch (e) {
        throw Error(failed(e));
      }
    },
    incomingHandler: {
      handle(request: IncomingRequest, out: ResponseOutparam): void {
        const req = toRequest(request);
        let response: ActorResponse;
        try {
          response = definition.fetch ? definition.fetch(req) : { status: 404, body: "no fetch handler\n" };
        } catch (e) {
          // The details (a thrown value's fields, a host error's) go to the
          // log only: a 500 body says nothing a caller should not see.
          console.error(failed(e));
          response = { status: 500, body: "actor error\n" };
        }
        respond(out, response);
        release(request);
      },
    },
  };
}
