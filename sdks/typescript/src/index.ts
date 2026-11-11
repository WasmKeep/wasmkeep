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
import * as wkStorage from "wasmkeep:actor/storage@0.2.0";
import * as wkAlarms from "wasmkeep:actor/alarms@0.2.0";
import * as wkActors from "wasmkeep:actor/actors@0.2.0";
import * as wkSockets from "wasmkeep:actor/websockets@0.2.0";
import * as wkEnv from "wasmkeep:actor/env@0.2.0";
import type { Value } from "wasmkeep:actor/types@0.2.0";
import {
  Fields,
  OutgoingBody,
  OutgoingResponse,
  ResponseOutparam,
  type IncomingRequest,
} from "wasi:http/types@0.2.12";

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

const encoder = new TextEncoder();
const decoder = new TextDecoder();

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

/** A request to another actor. */
export interface CallRequest {
  method?: string;
  path?: string;
  headers?: Record<string, string>;
  body?: string | Uint8Array;
}

/** Another actor's answer. */
export interface CallResponse {
  status: number;
  headers: Record<string, string>;
  body: Uint8Array;
  text(): string;
}

/** A call that did not get an answer. `kind`: invalid-address, reentrant,
 * too-large, timeout, failed. */
export class CallError extends Error {
  constructor(
    readonly kind: string,
    message: string,
  ) {
    super(message);
    this.name = "CallError";
  }
}

/** Configuration and secrets the operator gives this class
 * (`wasmkeep serve --env CLASS=FILE`). */
export const env = {
  /** The value of `name`, or `undefined` if it is not set. */
  get(name: string): string | undefined {
    // `none` is `null` on QuickJS: keep `undefined` on either engine.
    return wkEnv.get(name) ?? undefined;
  },
};

/** An actor's address. Within a tenant, `${class}/${name}` is what
 * `actors.call` takes. */
export interface ActorAddress {
  tenant: string;
  class: string;
  name: string;
}

export const actors = {
  /** This actor's own address (requests arrive with `/actor/<class>/<name>`
   * removed, so this is how an actor learns who it is). */
  self(): ActorAddress {
    const a = wkActors.selfAddress();
    return { tenant: a.tenant, class: a.class, name: a.name };
  },

  /** Calls actor `class/name` (same tenant) and waits for its response.
   * Other events to this actor wait meanwhile. */
  call(address: string, request: CallRequest = {}): CallResponse {
    const body =
      typeof request.body === "string" ? encoder.encode(request.body) : (request.body ?? new Uint8Array());
    let r: wkActors.Response;
    try {
      r = wkActors.call(address, {
        method: request.method ?? "GET",
        path: request.path ?? "/",
        headers: Object.entries(request.headers ?? {}),
        body,
      });
    } catch (e) {
      const p = (e as { payload?: { tag: string; val?: string } }).payload;
      if (p && typeof p.tag === "string") throw new CallError(p.tag, p.val ?? p.tag);
      throw e;
    }
    return {
      status: r.status,
      headers: Object.fromEntries(r.headers),
      body: r.body,
      text: () => decoder.decode(r.body),
    };
  },
};

// ------------------------------------------------------------- websockets

/** A WebSocket connection id. */
export type ConnId = bigint;

export const sockets = {
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

/** An incoming request. The path includes the query, relative to the
 * actor (`/actor/<class>/<name>` removed). */
export interface ActorRequest {
  method: string;
  path: string;
  headers: Record<string, string>;
  /** The body, read on first use. */
  bytes(): Uint8Array;
  text(): string;
  json<T = unknown>(): T;
}

/** What a handler returns: a body (status 200), or a full response. */
export type ActorResponse =
  | string
  | Uint8Array
  | {
      status?: number;
      headers?: Record<string, string>;
      body?: string | Uint8Array;
    };

/** A JSON response. */
export function json(value: unknown, status = 200): ActorResponse {
  return { status, headers: { "content-type": "application/json" }, body: JSON.stringify(value) };
}

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

function readAll(request: IncomingRequest): Uint8Array {
  const body = request.consume();
  const stream = body.stream();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    let chunk: Uint8Array;
    try {
      chunk = stream.blockingRead(65536n);
    } catch {
      break; // closed: the body has ended
    }
    chunks.push(chunk);
    total += chunk.length;
  }
  const out = new Uint8Array(total);
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.length;
  }
  return out;
}

function respond(out: ResponseOutparam, response: ActorResponse): void {
  // Bytes read from SQL on StarlingMonkey are Uint8Arrays of another realm
  // (not `instanceof Uint8Array` here): any typed-array view is a body.
  const r = typeof response === "string" || ArrayBuffer.isView(response) ? { body: response as string | Uint8Array } : response;
  const body = typeof r.body === "string" ? encoder.encode(r.body) : (r.body ?? new Uint8Array());
  const headers: Record<string, string> = { "content-type": "text/plain; charset=utf-8", ...r.headers };
  const fields = Fields.fromList(
    Object.entries(headers).map(([k, v]) => [k.toLowerCase(), encoder.encode(v)] as [string, Uint8Array]),
  );
  const outgoing = new OutgoingResponse(fields);
  outgoing.setStatusCode(r.status ?? 200);
  const outBody = outgoing.body();
  ResponseOutparam.set(out, { tag: "ok", val: outgoing });
  const stream = outBody.write();
  for (let at = 0; at < body.length; at += 4096) {
    stream.blockingWriteAndFlush(body.subarray(at, at + 4096));
  }
  (stream as unknown as Disposable)[Symbol.dispose]?.();
  OutgoingBody.finish(outBody, undefined);
}

function methodName(m: ReturnType<IncomingRequest["method"]>): string {
  return m.tag === "other" ? m.val : m.tag.toUpperCase();
}

/** The component exports for `definition`. Called by the build's generated
 * entry module; not needed in actor code. */
export function exportsFor(definition: ActorDefinition) {
  const failed = (e: unknown) => explainMissing(e instanceof Error ? e.message : String(e));
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
      definition.websocket?.message?.(conn, msg.val);
    },
    onWebsocketClose(conn: bigint, code: number, reason: string): void {
      definition.websocket?.close?.(conn, code, reason);
    },
    incomingHandler: {
      handle(request: IncomingRequest, out: ResponseOutparam): void {
        let body: Uint8Array | undefined;
        const req: ActorRequest = {
          method: methodName(request.method()),
          path: request.pathWithQuery() ?? "/",
          headers: Object.fromEntries(
            request.headers().entries().map(([k, v]) => [k, decoder.decode(v)]),
          ),
          bytes: () => (body ??= readAll(request)),
          text: () => decoder.decode(req.bytes()),
          json: <T>() => JSON.parse(req.text()) as T,
        };
        let response: ActorResponse;
        try {
          response = definition.fetch ? definition.fetch(req) : { status: 404, body: "no fetch handler\n" };
        } catch (e) {
          response = { status: 500, body: `${failed(e)}\n` };
        }
        respond(out, response);
      },
    },
  };
}
