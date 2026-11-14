// The database pattern's protocol (ADR 0029): the JSON a client sends to
// a database actor and the JSON it answers, byte for byte the Rust SDK's
// `db.rs`, plus the transaction-control check and the limits. Pure: no
// host imports, so the unit tests run it under node.

import type { Value } from "wasmkeep:actor/types@0.2.0";

export const READ = "/wasmkeep-db/v1/read";
export const WRITE = "/wasmkeep-db/v1/write";
export const PREFIX = "/wasmkeep-db/v1";

/** A value bound to or read from SQL. Integers that do not fit a JS number
 * exactly are `bigint`; a boolean binds as 1 or 0. */
export type SqlValue = null | number | bigint | string | Uint8Array | boolean;

/** A parameter: a `SqlValue`, or a value with its SQL type spelled out
 * (`{ tag: "real", val: 2 }` binds a real even though 2 is whole). */
export type Param = SqlValue | Value;

/** One statement: its SQL, the values bound to its placeholders, and
 * whether it answers its rows (`rows: true`: a `SELECT`, or a write with
 * `RETURNING`) or the number of rows it changed. */
export interface Statement {
  sql: string;
  params?: Param[];
  rows?: boolean;
}

/** The rows of a query: column names, then one array of values per row,
 * in column order. */
export interface Rows {
  columns: string[];
  rows: SqlValue[][];
}

/** What one statement answered. */
export type Outcome = Rows | { changes: number };

/** The caps a database applies to each request (the host's own limits
 * apply as well). */
export interface Limits {
  /** The request body (default 4 MiB). Blobs travel as hex. */
  maxRequestBytes: number;
  /** Statements in one request (default 100). */
  maxStatements: number;
  /** The SQL text of one statement, in UTF-8 bytes (default 100,000). */
  maxSqlBytes: number;
  /** Parameters of one statement (default 100). */
  maxParams: number;
  /** The encoded answer (default 8 MiB). */
  maxResponseBytes: number;
}

export const DEFAULT_LIMITS: Limits = {
  maxRequestBytes: 4 * 1024 * 1024,
  maxStatements: 100,
  maxSqlBytes: 100_000,
  maxParams: 100,
  maxResponseBytes: 8 * 1024 * 1024,
};

export type SqlErrorKind =
  | "syntax"
  | "constraint"
  | "bind"
  | "multiple-statements"
  | "quota-exceeded"
  | "not-allowed"
  | "internal";

const KINDS: readonly string[] = [
  "syntax",
  "constraint",
  "bind",
  "multiple-statements",
  "quota-exceeded",
  "not-allowed",
  "internal",
];

/** Why a database request failed, as `kind`:
 *
 * - `sql`: a statement failed in the database (`statement`, from 0;
 *   undefined when the commit failed), with the host's `sqlKind`. Nothing
 *   of the request was kept;
 * - `not-allowed`: statement `statement` controls the transaction
 *   (`BEGIN`, `COMMIT`, ...), refused before anything ran;
 * - `too-large`: the request or its answer is over a limit;
 * - `call`: the call failed. A write may then have been committed;
 * - `refused`: any other answer (`status`: 403 for a refused caller, 200
 *   for a malformed answer). */
export class DbError extends Error {
  readonly kind: "sql" | "not-allowed" | "too-large" | "call" | "refused";
  readonly sqlKind?: SqlErrorKind;
  readonly statement?: number;
  readonly status?: number;
  readonly detail: string;

  constructor(
    kind: DbError["kind"],
    detail: string,
    extra: { sqlKind?: SqlErrorKind; statement?: number; status?: number } = {},
  ) {
    super(
      kind === "sql"
        ? extra.statement === undefined
          ? `commit: ${extra.sqlKind} error: ${detail}`
          : `statement ${extra.statement}: ${extra.sqlKind} error: ${detail}`
        : kind === "not-allowed"
          ? `statement ${extra.statement}: ${detail}`
          : kind === "too-large"
            ? `too large: ${detail}`
            : kind === "call"
              ? detail
              : `database answered ${extra.status}: ${detail}`,
    );
    this.name = "DbError";
    this.kind = kind;
    this.detail = detail;
    if (extra.sqlKind !== undefined) this.sqlKind = extra.sqlKind;
    if (extra.statement !== undefined) this.statement = extra.statement;
    if (extra.status !== undefined) this.status = extra.status;
  }
}

// ---------------------------------------------------------------- values

const I64_MIN = -(2n ** 63n);
const I64_MAX = 2n ** 63n - 1n;
const DIGITS = "0123456789abcdef";

/** Whether `s` holds no lone surrogate (it is valid Unicode, as every
 * Rust string is). */
export function wellFormed(s: string): boolean {
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c >= 0xd800 && c <= 0xdbff) {
      const d = s.charCodeAt(i + 1);
      if (!(d >= 0xdc00 && d <= 0xdfff)) return false;
      i++;
    } else if (c >= 0xdc00 && c <= 0xdfff) {
      return false;
    }
  }
  return true;
}

function isTagged(v: unknown): v is Value {
  return typeof v === "object" && v !== null && !(v instanceof Uint8Array) && typeof (v as Value).tag === "string";
}

/** A parameter as a SQL value: a safe integer (up to 2^53 - 1) is an
 * integer, any other number a real; a tagged value as it is. */
export function toValue(v: Param | undefined): Value {
  if (v === null || v === undefined) return { tag: "null" };
  if (isTagged(v)) return v;
  if (typeof v === "boolean") return { tag: "integer", val: v ? 1n : 0n };
  if (typeof v === "bigint") return { tag: "integer", val: v };
  if (typeof v === "number") return Number.isSafeInteger(v) ? { tag: "integer", val: BigInt(v) } : { tag: "real", val: v };
  if (typeof v === "string") return { tag: "text", val: v };
  if (v instanceof Uint8Array) return { tag: "blob", val: v };
  throw new TypeError(`not a SQL value: ${typeof v}`);
}

/** A SQL value as a `SqlValue`: integers past 2^53 stay `bigint`. */
export function fromValue(v: Value): SqlValue {
  switch (v.tag) {
    case "null":
      return null;
    case "integer":
      return v.val >= BigInt(Number.MIN_SAFE_INTEGER) && v.val <= BigInt(Number.MAX_SAFE_INTEGER)
        ? Number(v.val)
        : v.val;
    case "real":
    case "text":
    case "blob":
      return v.val;
  }
}

/** A real as the wire spells it: JavaScript's `String`, with `-0`. */
function realString(v: number): string {
  return Object.is(v, -0) ? "-0" : String(v);
}

// Two digits per byte, looked up: blobs are encoded in a handler's CPU
// budget, and string concatenation per digit is slow on interpreters.
const BYTE_HEX: string[] = Array.from({ length: 256 }, (_, b) => DIGITS[b >> 4] + DIGITS[b & 0x0f]);

function hex(bytes: Uint8Array): string {
  const parts = new Array<string>(bytes.length);
  for (let i = 0; i < bytes.length; i++) parts[i] = BYTE_HEX[bytes[i]];
  return parts.join("");
}

function unhex(s: string): Uint8Array | undefined {
  if (s.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(s)) return undefined;
  const out = new Uint8Array(s.length / 2);
  for (let i = 0; i < out.length; i++) out[i] = parseInt(s.slice(2 * i, 2 * i + 2), 16);
  return out;
}

/** A value as it goes in JSON: `null`, a string for text, and
 * `{"integer": "<decimal>"}`, `{"real": "<number>"}` or
 * `{"blob": "<lowercase hex>"}`. Throws a RangeError for what the Rust
 * SDK cannot hold (an integer past 64 bits, text with a lone surrogate). */
export function wireValue(v: Value): unknown {
  switch (v.tag) {
    case "null":
      return null;
    case "text":
      if (!wellFormed(v.val)) throw new RangeError("text holds a lone surrogate");
      return v.val;
    case "integer":
      if (v.val < I64_MIN || v.val > I64_MAX) throw new RangeError(`${v.val} does not fit 64 bits`);
      return { integer: v.val.toString() };
    case "real":
      return { real: realString(v.val) };
    case "blob":
      return { blob: hex(v.val) };
  }
}

/** Rust's reading of a real: JavaScript's spellings only. */
function realFrom(s: string): number | undefined {
  if (s === "NaN") return NaN;
  if (s === "Infinity") return Infinity;
  if (s === "-Infinity") return -Infinity;
  if (!/^-?[0-9.eE+-]+$/.test(s) || !/[0-9]/.test(s) || s.startsWith("+")) return undefined;
  // Rust's f64 grammar, which `Number` would loosen (hex, spaces, "").
  if (!/^-?([0-9]+\.?[0-9]*|\.[0-9]+)([eE][+-]?[0-9]+)?$/.test(s)) return undefined;
  return Number(s);
}

/** A value read from JSON, or `undefined` if it is not one. */
export function valueFrom(j: unknown): Value | undefined {
  if (j === null) return { tag: "null" };
  if (typeof j === "string") return wellFormed(j) ? { tag: "text", val: j } : undefined;
  if (typeof j !== "object" || Array.isArray(j)) return undefined;
  const keys = Object.keys(j as object);
  if (keys.length !== 1) return undefined;
  const s = (j as Record<string, unknown>)[keys[0]];
  if (typeof s !== "string") return undefined;
  switch (keys[0]) {
    case "integer": {
      if (!/^-?(0|[1-9][0-9]*)$/.test(s) || s === "-0") return undefined;
      const v = BigInt(s);
      return v < I64_MIN || v > I64_MAX ? undefined : { tag: "integer", val: v };
    }
    case "real": {
      const v = realFrom(s);
      return v === undefined ? undefined : { tag: "real", val: v };
    }
    case "blob": {
      const v = unhex(s);
      return v === undefined ? undefined : { tag: "blob", val: v };
    }
    default:
      return undefined;
  }
}

// -------------------------------------------------------------- requests

/** The request body for `statements`, as the Rust client writes it. */
export function encodeRequest(statements: readonly Statement[]): string {
  return JSON.stringify({
    statements: statements.map((s) => {
      if (typeof s.sql !== "string") throw new TypeError("sql must be a string");
      if (!wellFormed(s.sql)) throw new RangeError("sql holds a lone surrogate");
      return { sql: s.sql, params: (s.params ?? []).map((p) => wireValue(toValue(p))), rows: s.rows === true };
    }),
  });
}

/** A statement as the handler runs it. */
export interface Decoded {
  sql: string;
  params: Value[];
  rows: boolean;
}

/** A refusal of a whole request: status, code, message. */
export interface Refusal {
  status: number;
  code: string;
  message: string;
}

function only(j: unknown, allowed: string[]): j is Record<string, unknown> {
  return (
    typeof j === "object" && j !== null && !Array.isArray(j) && Object.keys(j).every((k) => allowed.includes(k))
  );
}

/** UTF-8 length of `s` (as Rust's `str::len`). */
export function utf8Length(s: string): number {
  let n = 0;
  for (let i = 0; i < s.length; i++) {
    const c = s.charCodeAt(i);
    if (c < 0x80) n += 1;
    else if (c < 0x800) n += 2;
    else if (c >= 0xd800 && c <= 0xdbff && i + 1 < s.length) {
      n += 4;
      i++;
    } else n += 3;
  }
  return n;
}

const strict = new TextDecoder("utf-8", { fatal: true });

/** Reads a request body, as the Rust handler does. */
export function decodeRequest(body: Uint8Array, limits: Limits): Decoded[] | Refusal {
  const bad = (message: string): Refusal => ({ status: 400, code: "bad-request", message });
  const big = (message: string): Refusal => ({ status: 413, code: "too-large", message });
  let text: string;
  try {
    text = strict.decode(body);
  } catch {
    return bad("the body is not UTF-8");
  }
  let j: unknown;
  try {
    j = JSON.parse(text);
  } catch {
    return bad("the body is not JSON");
  }
  if (!only(j, ["statements"])) return bad('expected {"statements": [...]} and nothing else');
  const items = j.statements;
  if (!Array.isArray(items)) return bad("statements must be an array");
  if (items.length === 0) return bad("no statements");
  if (items.length > limits.maxStatements) {
    return big(`${items.length} statements, more than ${limits.maxStatements}`);
  }
  const out: Decoded[] = [];
  for (let i = 0; i < items.length; i++) {
    const item: unknown = items[i];
    const no = (m: string) => bad(`statement ${i}: ${m}`);
    if (!only(item, ["sql", "params", "rows"])) return no("expected sql, params and rows only");
    const sql = item.sql;
    if (typeof sql !== "string" || !wellFormed(sql)) return no("sql must be a string");
    let rows = false;
    if (item.rows === true) rows = true;
    else if (item.rows !== undefined && item.rows !== false) return no("rows must be true or false");
    let params: unknown[] = [];
    if (item.params !== undefined) {
      if (!Array.isArray(item.params)) return no("params must be an array");
      params = item.params;
    }
    if (utf8Length(sql) > limits.maxSqlBytes) {
      return big(`statement ${i}: SQL longer than ${limits.maxSqlBytes} bytes`);
    }
    if (params.length > limits.maxParams) return big(`statement ${i}: more than ${limits.maxParams} parameters`);
    const values: Value[] = [];
    for (const p of params) {
      const v = valueFrom(p);
      if (v === undefined) return no("a parameter is not a wire value");
      values.push(v);
    }
    out.push({ sql, params: values, rows });
  }
  return out;
}

/** An error answer: `{"error": code, "kind"?, "statement"?, "message"}`. */
export function errorBody(code: string, message: string, kind?: string, statement?: number): string {
  return JSON.stringify({ error: code, kind, statement, message });
}

/** One query's answer: `{"columns": [...], "rows": [[...], ...]}`, or
 * `undefined` once it passes `max` bytes on top of `used`. */
export function rowsBody(columns: string[], values: Value[][], used: number, max: number): string | undefined {
  const parts: string[] = [`{"columns":${JSON.stringify(columns)},"rows":[`];
  let size = used + utf8Length(parts[0]);
  for (let i = 0; i < values.length; i++) {
    const row = (i > 0 ? "," : "") + JSON.stringify(values[i].map(wireValue));
    size += utf8Length(row);
    if (size > max) return undefined;
    parts.push(row);
  }
  parts.push("]}");
  return parts.join("");
}

// --------------------------------------------------------------- answers

function count(j: unknown): number | undefined {
  return typeof j === "number" && Number.isSafeInteger(j) && j >= 0 ? j : undefined;
}

function decodeOutcome(j: unknown, rows: boolean): Outcome | undefined {
  if (!rows) {
    if (!only(j, ["changes"])) return undefined;
    const n = count(j.changes);
    return n === undefined ? undefined : { changes: n };
  }
  if (!only(j, ["columns", "rows"])) return undefined;
  const { columns, rows: list } = j;
  if (!Array.isArray(columns) || !columns.every((c) => typeof c === "string" && wellFormed(c))) return undefined;
  if (!Array.isArray(list)) return undefined;
  const out: SqlValue[][] = [];
  for (const row of list) {
    if (!Array.isArray(row) || row.length !== columns.length) return undefined;
    const values: SqlValue[] = [];
    for (const cell of row) {
      const v = valueFrom(cell);
      if (v === undefined) return undefined;
      values.push(fromValue(v));
    }
    out.push(values);
  }
  return { columns: columns as string[], rows: out };
}

function decodeError(status: number, j: unknown): DbError | undefined {
  if (!only(j, ["error", "kind", "statement", "message"])) return undefined;
  const { error, kind, message } = j;
  if (typeof message !== "string" || typeof error !== "string") return undefined;
  let statement: number | undefined;
  if (j.statement !== undefined) {
    statement = count(j.statement);
    if (statement === undefined) return undefined;
  }
  switch (error) {
    case "sql":
      if (typeof kind !== "string" || !KINDS.includes(kind)) return undefined;
      return new DbError("sql", message, { sqlKind: kind as SqlErrorKind, statement });
    case "not-allowed":
      if (statement === undefined) return undefined;
      return new DbError("not-allowed", message, { statement });
    case "too-large":
      return new DbError("too-large", message);
    default:
      return new DbError("refused", message, { status });
  }
}

const lossy = new TextDecoder();

function badReply(): DbError {
  return new DbError("refused", "malformed reply from database", { status: 200 });
}

/** Reads a database's answer to `statements`; throws `DbError`. */
export function decodeReply(status: number, body: Uint8Array, statements: readonly Statement[]): Outcome[] {
  let j: unknown;
  try {
    j = JSON.parse(strict.decode(body));
  } catch {
    j = undefined;
  }
  if (status !== 200) {
    throw (j !== undefined && decodeError(status, j)) || new DbError("refused", lossy.decode(body).trim(), { status });
  }
  if (!only(j, ["results"]) || !Array.isArray(j.results) || j.results.length !== statements.length) throw badReply();
  return j.results.map((r: unknown, i: number) => {
    const o = decodeOutcome(r, statements[i].rows === true);
    if (o === undefined) throw badReply();
    return o;
  });
}

// ---------------------------------------------------- transaction control

function space(c: string): boolean {
  return c === " " || c === "\t" || c === "\n" || c === "\f" || c === "\r" || c === ";";
}

/** Whether `sql`'s first keyword controls a transaction (`BEGIN`,
 * `COMMIT`, `END`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`), skipping
 * whitespace, comments and empty statements, as the Rust SDK's
 * `db::controls_transaction`. */
export function controlsTransaction(sql: string): boolean {
  let at = 0;
  for (;;) {
    const c = sql[at];
    if (c !== undefined && space(c)) at++;
    else if (c === "-" && sql[at + 1] === "-") {
      while (at < sql.length && sql[at] !== "\n") at++;
    } else if (c === "/" && sql[at + 1] === "*") {
      at += 2;
      while (at < sql.length && !(sql[at] === "*" && sql[at + 1] === "/")) at++;
      at += 2;
    } else break;
  }
  let end = at;
  // SQLite's identifier characters: letters, digits, `_`, `$`, and any
  // character from U+0080 (so `COMMITé` is a name, not `COMMIT`).
  while (end < sql.length && (/[A-Za-z0-9_$]/.test(sql[end]) || sql.charCodeAt(end) >= 0x80)) end++;
  // ASCII case only, as Rust's `eq_ignore_ascii_case`.
  const word = sql.slice(at, end).replace(/[A-Z]/g, (c) => c.toLowerCase());
  return ["begin", "commit", "end", "rollback", "savepoint", "release"].includes(word);
}
