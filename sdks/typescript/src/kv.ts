/**
 * Key-value storage built from actors (ADR 0015): the client side.
 *
 * Keys are spread over a fixed number of shard actors of one class
 * (`<class>/shard-0` .. `shard-<n-1>`), chosen by FNV-1a 64 of the key
 * modulo `n`. Each shard keeps its keys in its own SQLite database, so every
 * operation on a key is strongly consistent. The shard class is the Rust
 * SDK's ready-made shard, deployed from `examples/jobs/kv`; this client
 * speaks its protocol (`/wasmkeep-kv/v1`) over `actors.call`.
 *
 * ```ts
 * import { kv } from "@wasmkeep/sdk";
 *
 * const store = new kv.Client("kv", 8);
 * const version = store.put("user:42", "Ada");
 * const entry = store.get("user:42");                  // { value, version, expiresAt? }
 * store.compareAndSet("user:42", version, "Ada L.");   // throws KvError "conflict" if it moved
 * ```
 *
 * @module
 */

import { call } from "./call";
import {
  type Entry,
  type Expect,
  MAX_KEY_LEN,
  ReadCache,
  Reader,
  decodeEntry,
  decodeKeys,
  encodeExpect,
  hex,
  lossy,
  mergePages,
  shardFor,
  shardName,
  toBytes,
  toU32,
  toU64,
} from "./wire";

export { MAX_KEY_LEN, allows, shardFor, shardName, type Entry, type Expect } from "./wire";

const PREFIX = "/wasmkeep-kv/v1";
const EXPECT = "x-wasmkeep-kv-expect";
const TTL = "x-wasmkeep-kv-ttl-ms";
const DURABLE = "x-wasmkeep-kv-durable";

/** Options for `Client.putWith`. */
export interface PutOptions {
  /** The condition the write needs (default `"any"`: none). */
  expect?: Expect;
  /** Expire the key this many milliseconds after the write. */
  ttlMs?: number;
  /** Return only once the write is durable (in the bucket in a cluster,
   * fsynced on one node). Default `true`; `false` acknowledges once
   * committed on the shard's node: faster, but a node lost in that window
   * loses it. */
  durable?: boolean;
}

/** Why a KV operation failed, as `kind`:
 *
 * - `conflict`: a conditional write found another version (or a key where
 *   it expected none);
 * - `key-too-long`: the key is longer than `MAX_KEY_LEN` bytes;
 * - `storage`: the shard's database refused the operation;
 * - `call`: the call to the shard failed. The outcome of a write is then
 *   not known (it may have applied): re-read before retrying a conditional
 *   write;
 * - `refused`: the shard answered with an unexpected `status`. */
export class KvError extends Error {
  readonly kind: "conflict" | "key-too-long" | "storage" | "call" | "refused";
  /** The shard's status, for `refused`. */
  readonly status?: number;
  /** The detail: the call's error, or the shard's answer. */
  readonly detail: string;

  constructor(kind: KvError["kind"], detail = "", status?: number) {
    super(
      kind === "conflict"
        ? "conflict: the key changed"
        : kind === "key-too-long"
          ? `key longer than ${MAX_KEY_LEN} bytes`
          : kind === "storage"
            ? `storage: ${detail}`
            : kind === "call"
              ? detail
              : `shard answered ${status}: ${detail}`,
    );
    this.name = "KvError";
    this.kind = kind;
    this.detail = detail;
    if (status !== undefined) this.status = status;
  }
}

function badReply(): KvError {
  return new KvError("refused", "malformed reply from shard", 200);
}

function refused(status: number, body: Uint8Array): KvError {
  return new KvError("refused", lossy(body), status);
}

function checkKey(key: Uint8Array): void {
  if (key.length > MAX_KEY_LEN) throw new KvError("key-too-long");
}

// One per instance: lost with it (hibernation, a failed event, a deploy).
const CACHE = new ReadCache(4096);

/** Reaches the shards of one class. Cheap to create; keep one per class. */
export class Client {
  readonly class: string;
  readonly shards: number;
  private cacheMs = 0;

  /** Keys spread over `shards` actors of `cls` (at least one). Every caller
   * must use the same count. */
  constructor(cls: string, shards: number) {
    this.class = cls;
    this.shards = Math.max(1, toU32(shards, "shards"));
  }

  /** Serve `get` from an in-memory cache in this actor's instance when the
   * entry was read less than `maxAgeMs` ago. This actor's own writes
   * through the client update the cache, so it reads its own writes; other
   * actors' writes show up within `maxAgeMs`. Returns this client. */
  withReadCache(maxAgeMs: number): this {
    this.cacheMs = Number(toU64(maxAgeMs));
    return this;
  }

  private address(key: Uint8Array): string {
    return `${this.class}/${shardName(shardFor(key, this.shards))}`;
  }

  private cacheKey(key: Uint8Array): string {
    return `${this.class}\0${hex(key)}`;
  }

  private remember(key: Uint8Array, entry: Entry | undefined): void {
    if (this.cacheMs > 0) CACHE.insert(this.cacheKey(key), Date.now(), entry);
  }

  private forget(key: Uint8Array): void {
    if (this.cacheMs > 0) CACHE.remove(this.cacheKey(key));
  }

  /** The entry at `key` (`undefined` if absent or expired), from the read
   * cache if one is set and fresh. */
  get(key: string | Uint8Array): Entry | undefined {
    const k = toBytes(key);
    if (this.cacheMs > 0) {
      const hit = CACHE.get(this.cacheKey(k), Date.now(), this.cacheMs);
      if (hit !== undefined) return hit.entry;
    }
    return this.getFresh(k);
  }

  /** The entry at `key`, always asking the shard. */
  getFresh(key: string | Uint8Array): Entry | undefined {
    const k = toBytes(key);
    checkKey(k);
    const r = call(this.address(k), "GET", `${PREFIX}/k/${hex(k)}`, [], new Uint8Array());
    if ("error" in r) throw new KvError("call", r.error);
    let entry: Entry | undefined;
    if (r.reply.status === 200) {
      entry = decodeEntry(r.reply.body);
      if (entry === undefined) throw badReply();
    } else if (r.reply.status !== 404) {
      throw refused(r.reply.status, r.reply.body);
    }
    this.remember(k, entry && { ...entry, value: entry.value.slice() });
    return entry;
  }

  /** Stores `value` at `key` (durably); returns the new version. */
  put(key: string | Uint8Array, value: string | Uint8Array): number {
    return this.putWith(key, value, {});
  }

  /** Stores `value` only if `key` is at version `expected` (`undefined`:
   * absent); throws `KvError` `conflict` otherwise. */
  compareAndSet(key: string | Uint8Array, expected: number | undefined, value: string | Uint8Array): number {
    return this.putWith(key, value, { expect: expected === undefined ? "absent" : { version: expected } });
  }

  /** Stores `value` at `key` with `options`; returns the new version. */
  putWith(key: string | Uint8Array, value: string | Uint8Array, options: PutOptions = {}): number {
    const k = toBytes(key);
    const v = toBytes(value);
    checkKey(k);
    const headers: [string, string][] = [
      [EXPECT, encodeExpect(options.expect ?? "any")],
      [DURABLE, options.durable === false ? "0" : "1"],
    ];
    if (options.ttlMs !== undefined) headers.push([TTL, toU64(options.ttlMs).toString()]);
    const sent = Date.now();
    const r = call(this.address(k), "PUT", `${PREFIX}/k/${hex(k)}`, headers, v);
    if ("error" in r) {
      this.forget(k);
      throw new KvError("call", r.error);
    }
    if (r.reply.status === 200) {
      const raw = new Reader(r.reply.body).u64();
      if (raw === undefined || raw > BigInt(Number.MAX_SAFE_INTEGER)) throw badReply();
      const version = Number(raw);
      const entry: Entry = { value: v.slice(), version };
      if (options.ttlMs !== undefined) entry.expiresAt = sent + options.ttlMs;
      this.remember(k, entry);
      return version;
    }
    this.forget(k);
    if (r.reply.status === 409) throw new KvError("conflict");
    throw refused(r.reply.status, r.reply.body);
  }

  /** Removes `key`; `true` if it was there. */
  delete(key: string | Uint8Array): boolean {
    return this.deleteWith(key, "any");
  }

  /** Removes `key` only if it holds `version`; throws `KvError`
   * `conflict` otherwise. */
  deleteIf(key: string | Uint8Array, version: number): boolean {
    return this.deleteWith(key, { version });
  }

  private deleteWith(key: string | Uint8Array, expect: Expect): boolean {
    const k = toBytes(key);
    checkKey(k);
    const headers: [string, string][] = [[EXPECT, encodeExpect(expect)]];
    const r = call(this.address(k), "DELETE", `${PREFIX}/k/${hex(k)}`, headers, new Uint8Array());
    if ("error" in r) {
      this.forget(k);
      throw new KvError("call", r.error);
    }
    if (r.reply.status === 200) {
      const removed = new Reader(r.reply.body).u64();
      if (removed === undefined) throw badReply();
      this.remember(k, undefined);
      return removed === 1n;
    }
    this.forget(k);
    if (r.reply.status === 409) throw new KvError("conflict");
    throw refused(r.reply.status, r.reply.body);
  }

  /** Up to `limit` keys starting with `prefix`, in byte order, after
   * `after` (exclusive) if given: pass the last key of a page to get the
   * next one. Asks every shard (one call each), so it costs more than a
   * read; keep it for listings, not hot paths. */
  list(prefix: string | Uint8Array, after: string | Uint8Array | undefined, limit: number): Uint8Array[] {
    toU32(limit, "limit");
    let path = `${PREFIX}/list?prefix=${hex(toBytes(prefix))}&limit=${limit}`;
    if (after !== undefined) path += `&after=${hex(toBytes(after))}`;
    const pages: Uint8Array[][] = [];
    for (let index = 0; index < this.shards; index++) {
      const r = call(`${this.class}/${shardName(index)}`, "GET", path, [], new Uint8Array());
      if ("error" in r) throw new KvError("call", r.error);
      if (r.reply.status !== 200) throw refused(r.reply.status, r.reply.body);
      const keys = decodeKeys(r.reply.body);
      if (keys === undefined) throw badReply();
      pages.push(keys);
    }
    return mergePages(pages, limit);
  }
}
