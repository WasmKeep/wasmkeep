/**
 * A database kept by one actor (ADR 0029), like a Cloudflare D1 database:
 * an actor whose SQLite database holds an app's tables, one writer, run
 * for the app's other actors and its front over `actors.call`.
 *
 * ```ts
 * import { db, defineActor } from "@wasmkeep/sdk";
 *
 * // The database class (deployed as class `db`, `--access db=internal`):
 * export default db.define({
 *   migrations: ["CREATE TABLE todos (id INTEGER PRIMARY KEY, text TEXT NOT NULL)"],
 * });
 *
 * // Any other actor of the app:
 * const main = new db.Client("db/main");
 * main.exec("INSERT INTO todos (text) VALUES (?1)", ["buy milk"]);
 * const { columns, rows } = main.query("SELECT id, text FROM todos");
 * ```
 *
 * **SQL comes from your code, never from a client.** The handler runs
 * whatever SQL its callers send, and only the app's actors and front can
 * call it: it answers 403 to a request without the `x-wasmkeep-caller`
 * header the host vouches for. Never pass SQL text from an HTTP request to
 * a `Client`, and bind every value as a parameter (`?1`), never by
 * formatting it into the SQL.
 *
 * The wire format, the limits and the errors are the Rust SDK's
 * (`wasmkeep_sdk::db`), byte for byte: a client in either language talks
 * to a database class in either.
 *
 * @module
 */

import * as wkStorage from "wasmkeep:actor/storage@0.2.0";
import type { Rows as WitRows, Value } from "wasmkeep:actor/types@0.2.0";
import type { ActorDefinition, ActorRequest, ActorResponse } from "./index";
import {
  DEFAULT_LIMITS,
  type Decoded,
  type Limits,
  PREFIX,
  READ,
  WRITE,
  controlsTransaction,
  decodeRequest,
  limitsOf,
  errorBody,
  rowsBody,
  utf8Length,
} from "./db-wire";
import { header } from "./wire";

export { Client, objects } from "./db-client";
export {
  DEFAULT_LIMITS,
  DbError,
  controlsTransaction,
  type Limits,
  type Outcome,
  type Param,
  type Rows,
  type SqlErrorKind,
  type SqlValue,
  type Statement,
} from "./db-wire";

/** How a database class answers. */
export interface HandlerOptions {
  /** Caps on each request; the rest stay at `DEFAULT_LIMITS`. */
  limits?: Partial<Limits>;
  /** Refuses (403) callers for which this returns false. It gets the
   * caller's `tenant/class/name` as the host sends it, the name
   * percent-encoded (`room%201`; the front is `tenant/_front/main`). */
  allow?: (caller: string) => boolean;
}

interface SqlFailure {
  kind: string;
  message: string;
}

/** What a host storage call threw, as `{ kind, message }`. */
function failure(e: unknown): SqlFailure {
  const p = (e as { payload?: { kind?: string; message?: string } }).payload;
  if (p && typeof p.message === "string") return { kind: p.kind ?? "internal", message: p.message };
  return { kind: "internal", message: e instanceof Error ? e.message : String(e) };
}

function answer(status: number, body: string): ActorResponse {
  return { status, headers: { "content-type": "application/json" }, body };
}

function sqlError(f: SqlFailure, statement?: number): ActorResponse {
  return answer(f.kind === "internal" ? 500 : 400, errorBody("sql", f.message, f.kind, statement));
}

function tooLarge(limits: Limits): ActorResponse {
  return answer(413, errorBody("too-large", `answer over ${limits.maxResponseBytes} bytes`));
}

function rollback(): void {
  try {
    wkStorage.exec("ROLLBACK", []);
  } catch {
    // A failure that already ended the transaction: nothing to undo.
  }
}

/** Runs a request as one transaction: committed durably (a write) or
 * rolled back (a read), and rolled back at the first failure. */
function run(limits: Limits, write: boolean, statements: Decoded[]): ActorResponse {
  const control = statements.findIndex((s) => controlsTransaction(s.sql));
  if (control >= 0) {
    const message =
      "transaction control (BEGIN, COMMIT, END, ROLLBACK, SAVEPOINT, RELEASE) is not allowed: " +
      "each request is one transaction already";
    return answer(400, errorBody("not-allowed", message, undefined, control));
  }
  try {
    wkStorage.exec("BEGIN", []);
  } catch (e) {
    return sqlError(failure(e));
  }
  const parts: string[] = ['{"results":['];
  let size = parts[0].length;
  for (let i = 0; i < statements.length; i++) {
    const s = statements[i];
    let part: string | undefined;
    try {
      if (s.rows) {
        const rows: WitRows = wkStorage.query(s.sql, s.params);
        part = rowsBody(rows.columns, rows.values, size, limits.maxResponseBytes);
      } else {
        part = `{"changes":${wkStorage.exec(s.sql, s.params).toString()}}`;
      }
    } catch (e) {
      rollback();
      return sqlError(failure(e), i);
    }
    if (part === undefined) {
      rollback();
      return tooLarge(limits);
    }
    if (i > 0) part = "," + part;
    size += utf8Length(part);
    if (size > limits.maxResponseBytes) {
      rollback();
      return tooLarge(limits);
    }
    parts.push(part);
  }
  parts.push("]}");
  try {
    if (write) wkStorage.execDurable("COMMIT", []);
    else wkStorage.exec("ROLLBACK", []);
  } catch (e) {
    if (!write) return sqlError(failure(e));
    // A transaction still open was not committed: undo it. If none is, the
    // commit happened locally and only its durability failed: the write may
    // be kept, so say that, not "nothing".
    try {
      wkStorage.exec("ROLLBACK", []);
    } catch {
      return answer(500, errorBody("unknown-outcome", `the commit may have been kept: ${failure(e).message}`));
    }
    return sqlError(failure(e));
  }
  return answer(200, parts.join(""));
}

/** Serves the client's protocol: call it from `fetch`. Requests from
 * outside clients (without `x-wasmkeep-caller`) get 403: only the app's
 * actors and its front may use the database. */
export function handle(req: ActorRequest, options: HandlerOptions = {}): ActorResponse {
  const limits = limitsOf(options.limits);
  const caller = header(req.headers, "x-wasmkeep-caller");
  if (caller === undefined || (options.allow !== undefined && !options.allow(caller))) {
    return answer(403, errorBody("forbidden", "only the app's actors and its front may use this database"));
  }
  let write: boolean;
  if (req.path === READ) write = false;
  else if (req.path === WRITE) write = true;
  else if (req.path.startsWith(PREFIX)) return answer(404, errorBody("not-found", `no such path ${req.path}`));
  else return answer(404, errorBody("not-found", "not a database path"));
  if (req.method !== "POST") return answer(405, errorBody("method", "use POST"));
  const body = req.bytes(limits.maxRequestBytes);
  if (body === undefined) {
    return answer(413, errorBody("too-large", `request body over ${limits.maxRequestBytes} bytes`));
  }
  const decoded = decodeRequest(body, limits);
  if (!Array.isArray(decoded)) return answer(decoded.status, errorBody(decoded.code, decoded.message));
  return run(limits, write, decoded);
}

/** Thrown by `migrate`. */
export class MigrationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "MigrationError";
  }
}

/** Applies the schema: `migrations` are statements, in order, each applied
 * once (in its own durable transaction) and recorded in the table
 * `wasmkeep_db_migrations`. Call it from `init`; returns how many it
 * applied. Only ever append to the list: a recorded migration whose text
 * changed, or more recorded migrations than listed, throws, and nothing is
 * applied. */
export function migrate(migrations: readonly string[]): number {
  const sql = (f: () => unknown, what: string) => {
    try {
      return f();
    } catch (e) {
      throw new MigrationError(`${what}: ${failure(e).message}`);
    }
  };
  sql(
    () =>
      wkStorage.exec(
        `CREATE TABLE IF NOT EXISTS wasmkeep_db_migrations (
            version INTEGER PRIMARY KEY,
            sql TEXT NOT NULL,
            applied_at INTEGER NOT NULL
        )`,
        [],
      ),
    "migrations table",
  );
  const applied = sql(
    () => wkStorage.query("SELECT sql FROM wasmkeep_db_migrations ORDER BY version", []),
    "migrations table",
  ) as WitRows;
  if (applied.values.length > migrations.length) {
    throw new MigrationError(
      `the database has ${applied.values.length} migrations applied, this component lists ${migrations.length}`,
    );
  }
  applied.values.forEach((row, i) => {
    const v: Value | undefined = row[0];
    if (v === undefined || v.tag !== "text" || v.val !== migrations[i]) {
      throw new MigrationError(
        `migration ${i} differs from the one applied: append new migrations instead of editing old ones`,
      );
    }
  });
  const now = BigInt(Date.now());
  for (let i = applied.values.length; i < migrations.length; i++) {
    const m = migrations[i];
    if (controlsTransaction(m)) throw new MigrationError(`migration ${i}: a migration may not control the transaction`);
    try {
      wkStorage.exec("BEGIN", []);
      wkStorage.exec(m, []);
      wkStorage.exec("INSERT INTO wasmkeep_db_migrations (version, sql, applied_at) VALUES (?1, ?2, ?3)", [
        { tag: "integer", val: BigInt(i) },
        { tag: "text", val: m },
        { tag: "integer", val: now },
      ]);
      wkStorage.execDurable("COMMIT", []);
    } catch (e) {
      rollback();
      throw new MigrationError(`migration ${i}: ${failure(e).message}`);
    }
  }
  return migrations.length - applied.values.length;
}

/** A database class: the whole actor definition. */
export function define(options: HandlerOptions & { migrations?: readonly string[] } = {}): ActorDefinition {
  const migrations = options.migrations ?? [];
  return {
    init: () => {
      migrate(migrations);
    },
    fetch: (req) => handle(req, options),
  };
}
