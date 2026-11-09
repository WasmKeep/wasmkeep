// The small binary framing the `kv` and `queue` clients speak with the
// actors that hold the data (the Rust SDK's `wire.rs`, byte for byte):
// unsigned integers as 8 bytes big-endian, byte strings as a 4-byte
// big-endian length then the bytes. Also the codecs of each message, the
// FNV-1a shard hash and the KV read cache.
//
// Pure: no host imports, so the unit tests run it under node. Internal to
// the SDK; the shapes follow the protocol in ADR 0015.

const U64_MAX = 0xffff_ffff_ffff_ffffn;
const U32_MAX = 0xffff_ffff;

// ------------------------------------------------------------------ framing

/** Appends to a buffer. */
export class Writer {
  private chunks: Uint8Array[] = [];
  private length = 0;

  /** An unsigned 64-bit integer (a non-negative safe integer or bigint). */
  u64(v: number | bigint): this {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigUint64(0, toU64(v));
    return this.push(b);
  }

  /** A byte string: its length (4 bytes), then the bytes. */
  bytes(v: Uint8Array): this {
    const len = new Uint8Array(4);
    // Bodies are capped at 16 MB by `actors.call`, so a length always fits.
    new DataView(len.buffer).setUint32(0, Math.min(v.length, U32_MAX));
    this.push(len);
    return this.push(v);
  }

  finish(): Uint8Array {
    const out = new Uint8Array(this.length);
    let at = 0;
    for (const c of this.chunks) {
      out.set(c, at);
      at += c.length;
    }
    return out;
  }

  private push(b: Uint8Array): this {
    this.chunks.push(b);
    this.length += b.length;
    return this;
  }
}

/** Reads what `Writer` wrote. Every read fails (`undefined`) past the end. */
export class Reader {
  private at = 0;
  private readonly view: DataView;

  constructor(private readonly buf: Uint8Array) {
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }

  u64(): bigint | undefined {
    if (this.buf.length - this.at < 8) return undefined;
    const v = this.view.getBigUint64(this.at);
    this.at += 8;
    return v;
  }

  bytes(): Uint8Array | undefined {
    if (this.buf.length - this.at < 4) return undefined;
    const len = this.view.getUint32(this.at);
    if (this.buf.length - this.at - 4 < len) return undefined;
    this.at += 4;
    const v = this.buf.slice(this.at, this.at + len);
    this.at += len;
    return v;
  }

  isEmpty(): boolean {
    return this.at >= this.buf.length;
  }
}

/** `v` as a u64, or a RangeError (Rust's types rule these out). */
export function toU64(v: number | bigint): bigint {
  if (typeof v === "number" && !Number.isSafeInteger(v)) {
    throw new RangeError(`${v} is not a safe integer`);
  }
  const b = BigInt(v);
  if (b < 0n || b > U64_MAX) throw new RangeError(`${v} does not fit 64 unsigned bits`);
  return b;
}

/** `v` checked as a u32 (a count or a limit). */
export function toU32(v: number, what: string): number {
  if (!Number.isInteger(v) || v < 0 || v > U32_MAX) {
    throw new RangeError(`${what} must be an integer from 0 to ${U32_MAX}, got ${v}`);
  }
  return v;
}

/** A u64 that must be exact as a number (ids, versions): `undefined` past
 * 2^53 - 1, which a reply then counts as malformed. */
function exact(v: bigint | undefined): number | undefined {
  return v !== undefined && v <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(v) : undefined;
}

/** A u64 as a number, rounded past 2^53 (times and counts). */
function approx(v: bigint | undefined): number | undefined {
  return v === undefined ? undefined : Number(v);
}

/** A u64 that must fit a u32 (an attempt), as Rust's `u32::try_from`. */
function u32(v: bigint | undefined): number | undefined {
  return v !== undefined && v <= BigInt(U32_MAX) ? Number(v) : undefined;
}

const DIGITS = "0123456789abcdef";

/** Lowercase hex, so any key fits in a path segment. */
export function hex(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += DIGITS[b >> 4] + DIGITS[b & 0x0f];
  return s;
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** A key, value or body as bytes: strings are UTF-8 (as Rust's `&str`). */
export function toBytes(v: string | Uint8Array): Uint8Array {
  return typeof v === "string" ? encoder.encode(v) : v;
}

/** A refusal's text: the body as lossy UTF-8, trimmed. */
export function lossy(body: Uint8Array): string {
  return decoder.decode(body).trim();
}

/** Byte-wise order, as Rust sorts `Vec<u8>`. */
export function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const n = Math.min(a.length, b.length);
  for (let i = 0; i < n; i++) {
    if (a[i] !== b[i]) return a[i] - b[i];
  }
  return a.length - b.length;
}

/** A header's value (first occurrence, case-insensitive). */
export function header(headers: Record<string, string>, name: string): string | undefined {
  const lower = name.toLowerCase();
  for (const [k, v] of Object.entries(headers)) {
    if (k.toLowerCase() === lower) return v;
  }
  return undefined;
}

// ----------------------------------------------------------------------- kv

/** Keys are at most this long (bytes). */
export const MAX_KEY_LEN = 1024;

/** A stored value. */
export interface Entry {
  /** The bytes stored. */
  value: Uint8Array;
  /** Changes on every write of the key, and is never reused within a shard
   * (not even after a delete), so a compare-and-set on an old version
   * always loses. */
  version: number;
  /** When the key expires (ms since the Unix epoch), if it does. */
  expiresAt?: number;
}

/** What a conditional write expects to find: anything, no key (or an
 * expired one), or this version. */
export type Expect = "any" | "absent" | { version: number };

/** Whether a key currently at `current` (`undefined`: absent) meets `expect`. */
export function allows(expect: Expect, current: number | undefined): boolean {
  if (expect === "any") return true;
  if (expect === "absent") return current === undefined;
  return current === expect.version;
}

export function encodeExpect(expect: Expect): string {
  return typeof expect === "string" ? expect : toU64(expect.version).toString();
}

export function decodeExpect(s: string): Expect | undefined {
  if (s === "any" || s === "absent") return s;
  // Rust's `u64::from_str`: digits, an optional leading `+`.
  if (!/^\+?[0-9]+$/.test(s)) return undefined;
  const v = BigInt(s);
  return v <= BigInt(Number.MAX_SAFE_INTEGER) ? { version: Number(v) } : undefined;
}

/** The shard (0-based) that holds `key` when there are `shards` of them:
 * FNV-1a (64 bits) of the key, modulo the count (zero counts as one). Part
 * of the data layout: it never changes, and matches the Rust SDK. */
export function shardFor(key: string | Uint8Array, shards: number): number {
  let hash = 0xcbf2_9ce4_8422_2325n;
  for (const b of toBytes(key)) {
    hash = ((hash ^ BigInt(b)) * 0x0000_0100_0000_01b3n) & U64_MAX;
  }
  return Number(hash % BigInt(Math.max(1, toU32(shards, "shards"))));
}

/** The actor name of shard `index`: `shard-<index>`. */
export function shardName(index: number): string {
  return `shard-${index}`;
}

export function encodeEntry(entry: Entry): Uint8Array {
  return new Writer()
    .u64(entry.version)
    .u64(entry.expiresAt ?? 0)
    .bytes(entry.value)
    .finish();
}

export function decodeEntry(buf: Uint8Array): Entry | undefined {
  const r = new Reader(buf);
  const version = exact(r.u64());
  const expires = approx(r.u64());
  const value = r.bytes();
  if (version === undefined || expires === undefined || value === undefined || !r.isEmpty()) {
    return undefined;
  }
  return expires === 0 ? { value, version } : { value, version, expiresAt: expires };
}

export function encodeKeys(keys: Uint8Array[]): Uint8Array {
  const w = new Writer().u64(keys.length);
  for (const k of keys) w.bytes(k);
  return w.finish();
}

export function decodeKeys(buf: Uint8Array): Uint8Array[] | undefined {
  const r = new Reader(buf);
  const n = r.u64();
  if (n === undefined) return undefined;
  const keys: Uint8Array[] = [];
  for (let i = 0n; i < n; i++) {
    const k = r.bytes();
    if (k === undefined) return undefined;
    keys.push(k);
  }
  return r.isEmpty() ? keys : undefined;
}

/** Merges sorted pages from every shard into one sorted page of `limit`. */
export function mergePages(pages: Uint8Array[][], limit: number): Uint8Array[] {
  return pages.flat().sort(compareBytes).slice(0, limit);
}

/** A bounded map of recent reads. When full, expired entries go first,
 * then everything: simple, and a full cache is refilled by the next reads. */
export class ReadCache {
  readonly entries = new Map<string, { at: number; entry: Entry | undefined }>();

  constructor(readonly capacity: number) {}

  /** `{ entry }` if read less than `maxAge` ago (an entry that expired since
   * reads as absent); `undefined` on a miss. A miss differs from a cached
   * absence (`{ entry: undefined }`). */
  get(key: string, now: number, maxAge: number): { entry: Entry | undefined } | undefined {
    const hit = this.entries.get(key);
    if (hit === undefined || Math.max(0, now - hit.at) >= maxAge) return undefined;
    const e = hit.entry;
    if (e === undefined || (e.expiresAt !== undefined && e.expiresAt <= now)) return { entry: undefined };
    return { entry: { ...e, value: e.value.slice() } };
  }

  insert(key: string, now: number, entry: Entry | undefined): void {
    if (this.entries.size >= this.capacity && !this.entries.has(key)) {
      for (const [k, { entry: e }] of this.entries) {
        if (e !== undefined && e.expiresAt !== undefined && e.expiresAt <= now) this.entries.delete(k);
      }
      if (this.entries.size >= this.capacity) this.entries.clear();
    }
    this.entries.set(key, { at: now, entry });
  }

  remove(key: string): void {
    this.entries.delete(key);
  }
}

// -------------------------------------------------------------------- queue

/** A leased or delivered message. */
export interface Message {
  /** Unique within the queue, never reused, the same on every delivery. */
  id: number;
  /** Which delivery this is, from 1. Acks and retries name it, so one for an
   * earlier lease of a message leased again is ignored. */
  attempt: number;
  /** When it was sent (ms since the Unix epoch). */
  enqueuedAt: number;
  /** What was sent. */
  body: Uint8Array;
}

/** What a consumer did with a message: `"ack"` (handled: delete it),
 * `"retry"` (deliver it again after the queue's backoff), or
 * `{ retryAfterMs }` (deliver it again after that delay); a retry of a
 * message that had all its attempts dead-letters it. */
export type Outcome = "ack" | "retry" | { retryAfterMs: number };

/** One settlement: a lease (`id`, `attempt`) and its outcome. */
export interface Settlement {
  id: number;
  attempt: number;
  outcome: Outcome;
}

/** How many messages are where. */
export interface Stats {
  /** Visible now: the next lease or delivery takes them. */
  ready: number;
  /** Sent with a delay that has not passed. */
  delayed: number;
  /** Leased and not yet acked, or waiting for a retry. */
  inFlight: number;
  /** In the dead-letter table. */
  dead: number;
}

/** A dead-lettered message. */
export interface DeadLetter {
  /** The message as it was last delivered. */
  message: Message;
  /** When it was dead-lettered (ms since the Unix epoch). */
  failedAt: number;
  /** Why: attempts exhausted, or the last delivery error. */
  reason: string;
}

function readMessage(r: Reader): Message | undefined {
  const id = exact(r.u64());
  const attempt = u32(r.u64());
  const enqueuedAt = approx(r.u64());
  const body = r.bytes();
  if (id === undefined || attempt === undefined || enqueuedAt === undefined || body === undefined) {
    return undefined;
  }
  return { id, attempt, enqueuedAt, body };
}

function writeMessage(w: Writer, m: Message): Writer {
  return w.u64(m.id).u64(m.attempt).u64(m.enqueuedAt).bytes(m.body);
}

/** Reads a count, then `n` items with `item`; `undefined` if any is
 * malformed or bytes are left over. */
function list<T>(buf: Uint8Array, item: (r: Reader) => T | undefined): T[] | undefined {
  const r = new Reader(buf);
  const n = r.u64();
  if (n === undefined) return undefined;
  const out: T[] = [];
  for (let i = 0n; i < n; i++) {
    const v = item(r);
    if (v === undefined) return undefined;
    out.push(v);
  }
  return r.isEmpty() ? out : undefined;
}

export function encodeMessages(messages: Message[]): Uint8Array {
  const w = new Writer().u64(messages.length);
  for (const m of messages) writeMessage(w, m);
  return w.finish();
}

export function decodeMessages(buf: Uint8Array): Message[] | undefined {
  return list(buf, readMessage);
}

/** `u64::MAX` in the delay slot means "the queue's backoff". */
export function encodeSettlements(items: Settlement[]): Uint8Array {
  const w = new Writer().u64(items.length);
  for (const { id, attempt, outcome } of items) {
    const [kind, delay] =
      outcome === "ack" ? [0, 0n] : outcome === "retry" ? [1, U64_MAX] : [1, toU64(outcome.retryAfterMs)];
    w.u64(id).u64(toU32(attempt, "attempt")).u64(kind).u64(delay);
  }
  return w.finish();
}

export function decodeSettlements(buf: Uint8Array): Settlement[] | undefined {
  return list(buf, (r) => {
    const id = exact(r.u64());
    const attempt = u32(r.u64());
    const kind = r.u64();
    const delay = r.u64();
    if (id === undefined || attempt === undefined || kind === undefined || delay === undefined) {
      return undefined;
    }
    let outcome: Outcome;
    if (kind === 0n) outcome = "ack";
    else if (kind === 1n && delay === U64_MAX) outcome = "retry";
    else if (kind === 1n) outcome = { retryAfterMs: Number(delay) };
    else return undefined;
    return { id, attempt, outcome };
  });
}

export function encodeCounts(counts: (number | bigint)[]): Uint8Array {
  const w = new Writer().u64(counts.length);
  for (const c of counts) w.u64(c);
  return w.finish();
}

/** Counts as bigints (ids are checked by the caller). */
export function decodeCounts(buf: Uint8Array): bigint[] | undefined {
  return list(buf, (r) => r.u64());
}

export function encodeBodies(bodies: Uint8Array[]): Uint8Array {
  const w = new Writer().u64(bodies.length);
  for (const b of bodies) w.bytes(b);
  return w.finish();
}

export function decodeBodies(buf: Uint8Array): Uint8Array[] | undefined {
  return list(buf, (r) => r.bytes());
}

export function encodeDead(items: DeadLetter[]): Uint8Array {
  const w = new Writer().u64(items.length);
  for (const d of items) writeMessage(w, d.message).u64(d.failedAt).bytes(encoder.encode(d.reason));
  return w.finish();
}

const strict = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });

export function decodeDead(buf: Uint8Array): DeadLetter[] | undefined {
  return list(buf, (r) => {
    const message = readMessage(r);
    const failedAt = approx(r.u64());
    const reason = r.bytes();
    if (message === undefined || failedAt === undefined || reason === undefined) return undefined;
    try {
      // Rust's `String::from_utf8`: invalid UTF-8 is malformed.
      return { message, failedAt, reason: strict.decode(reason) };
    } catch {
      return undefined;
    }
  });
}

/** Ids from a reply: exact numbers, or `undefined` if one is not. */
export function exactAll(values: bigint[]): number[] | undefined {
  const out: number[] = [];
  for (const v of values) {
    const n = exact(v);
    if (n === undefined) return undefined;
    out.push(n);
  }
  return out;
}
