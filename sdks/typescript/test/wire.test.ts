// The framing, codecs, shard hash and read cache: the Rust SDK's unit tests
// (wire.rs, kv.rs, queue.rs), plus golden bytes and shard choices computed
// from the Rust code.

import { test } from "node:test";
import assert from "node:assert/strict";
import {
  ReadCache,
  Reader,
  Writer,
  allows,
  decodeBodies,
  decodeCounts,
  decodeDead,
  decodeEntry,
  decodeExpect,
  decodeKeys,
  decodeMessages,
  decodeSettlements,
  encodeBodies,
  encodeCounts,
  encodeDead,
  encodeEntry,
  encodeExpect,
  encodeKeys,
  encodeMessages,
  encodeSettlements,
  hex,
  mergePages,
  shardFor,
  type Entry,
  type Expect,
  type Message,
  type Settlement,
} from "../src/wire";

const enc = new TextEncoder();
const b = (s: string) => enc.encode(s);
const unhex = (s: string) => Uint8Array.from(s.match(/../g) ?? [], (h) => parseInt(h, 16));
const U64_MAX = 0xffff_ffff_ffff_ffffn;

test("framing round-trips and rejects truncation", () => {
  const buf = new Writer().u64(7).bytes(b("abc")).bytes(b("")).u64(U64_MAX).finish();
  const r = new Reader(buf);
  assert.equal(r.u64(), 7n);
  assert.deepEqual(r.bytes(), b("abc"));
  assert.deepEqual(r.bytes(), b(""));
  assert.equal(r.u64(), U64_MAX);
  assert(r.isEmpty());
  assert.equal(r.u64(), undefined);
  // A length that runs past the end is refused, not truncated.
  assert.equal(new Reader(buf.subarray(8, 12)).bytes(), undefined);
});

test("framing bytes are big-endian, as Rust writes them", () => {
  const buf = new Writer().u64(7).bytes(b("abc")).u64(0x0102030405060708n).finish();
  assert.equal(hex(buf), "0000000000000007" + "00000003616263" + "0102030405060708");
  // Out-of-range integers are refused rather than wrapped.
  assert.throws(() => new Writer().u64(-1), RangeError);
  assert.throws(() => new Writer().u64(2 ** 53), RangeError);
  assert.throws(() => new Writer().u64(U64_MAX + 1n), RangeError);
});

test("hex is lowercase, two digits per byte", () => {
  const all = Uint8Array.from({ length: 256 }, (_, i) => i);
  assert.equal(hex(all).length, 512);
  assert.deepEqual(unhex(hex(all)), all);
  assert.equal(hex(Uint8Array.of(0, 255, 0x41)), "00ff41");
  assert.equal(hex(new Uint8Array()), "");
});

test("the shard hash is FNV-1a 64 and matches the Rust SDK", () => {
  // Known FNV-1a 64 values: "" and "a" (kv.rs).
  assert.equal(shardFor("", 0xffff_ffff), Number(0xcbf2_9ce4_8422_2325n % 0xffff_ffffn));
  assert.equal(shardFor("a", 1 << 16), Number(0xaf63_dc4c_8601_ec8cn % (1n << 16n)));
  assert.equal(shardFor("anything", 1), 0);
  assert.equal(shardFor("anything", 0), 0, "zero shards means one");
  // Computed with the Rust `shard_for`: [key, n=4, n=8, n=1000, n=u32::MAX].
  const vectors: [string | Uint8Array, number, number, number, number][] = [
    ["", 1, 5, 37, 1343537162],
    ["a", 0, 4, 996, 895863001],
    ["user:42", 2, 2, 410, 1223114855],
    ["user:0", 0, 0, 736, 1829090531],
    ["user:1", 3, 3, 947, 1829091222],
    ["user:2", 2, 6, 158, 1829091913],
    ["job:1", 1, 1, 481, 310798266],
    ["greeting", 2, 6, 198, 2089897798],
    ["clé é", 2, 6, 550, 819157025],
    [Uint8Array.of(0, 255, 128), 0, 0, 392, 1085081017],
  ];
  for (const [key, n4, n8, n1000, max] of vectors) {
    assert.deepEqual(
      [shardFor(key, 4), shardFor(key, 8), shardFor(key, 1000), shardFor(key, 0xffff_ffff)],
      [n4, n8, n1000, max],
      `key ${String(key)}`,
    );
  }
  // A string key hashes as its UTF-8 bytes.
  assert.equal(shardFor("clé é", 1000), shardFor(b("clé é"), 1000));
  // Spread: 1,000 keys over 8 shards, the exact counts Rust gets.
  const seen = new Array(8).fill(0);
  for (let i = 0; i < 1000; i++) seen[shardFor(`user:${i}`, 8)]++;
  assert.deepEqual(seen, [126, 125, 125, 126, 124, 125, 125, 124]);
  assert.throws(() => shardFor("k", -1), RangeError);
});

test("expectations", () => {
  assert(allows("any", undefined));
  assert(allows("any", 3));
  assert(allows("absent", undefined));
  assert(!allows("absent", 3));
  assert(allows({ version: 3 }, 3));
  assert(!allows({ version: 3 }, 4));
  assert(!allows({ version: 3 }, undefined));
  const all: Expect[] = ["any", "absent", { version: 0 }, { version: Number.MAX_SAFE_INTEGER }];
  for (const e of all) assert.deepEqual(decodeExpect(encodeExpect(e)), e);
  assert.equal(encodeExpect({ version: 42 }), "42");
  assert.equal(decodeExpect("nope"), undefined);
  assert.equal(decodeExpect("-1"), undefined);
});

test("entries and key lists round-trip", () => {
  const e: Entry = { value: b("v"), version: 9, expiresAt: 1234 };
  assert.deepEqual(decodeEntry(encodeEntry(e)), e);
  assert.equal(hex(encodeEntry(e)), "0000000000000009" + "00000000000004d2" + "0000000176");
  const forever: Entry = { value: b("v"), version: 9 };
  assert.deepEqual(decodeEntry(encodeEntry(forever)), forever);
  // Trailing bytes are refused.
  assert.equal(decodeEntry(Uint8Array.of(...encodeEntry(e), 0)), undefined);
  const keys = [b("a"), new Uint8Array(), Uint8Array.of(0, 255)];
  assert.deepEqual(decodeKeys(encodeKeys(keys)), keys);
  assert.equal(decodeKeys(Uint8Array.of(0, 0, 0, 0, 0, 0, 0, 1)), undefined);
  // A version past 2^53 - 1 cannot be a number: the reply is malformed.
  const big = new Writer().u64(2n ** 53n).u64(0).bytes(b("v")).finish();
  assert.equal(decodeEntry(big), undefined);
});

test("pages from shards merge in byte order", () => {
  const pages = [[b("a"), b("d")], [b("b"), b("c"), b("e")], []];
  assert.deepEqual(mergePages(pages, 4), [b("a"), b("b"), b("c"), b("d")]);
  // Byte order, not UTF-16 order: "é" (c3 a9) sorts after "z" (7a), a
  // prefix before its extensions.
  assert.deepEqual(mergePages([[b("é")], [b("z"), b("za")], [b("")]], 10), [b(""), b("z"), b("za"), b("é")]);
});

test("the read cache ages, expires and stays bounded", () => {
  const c = new ReadCache(2);
  const e = (v: number, exp?: number): Entry =>
    exp === undefined ? { value: new Uint8Array(), version: v } : { value: new Uint8Array(), version: v, expiresAt: exp };
  c.insert("k", 100, e(1));
  assert.deepEqual(c.get("k", 150, 100), { entry: e(1) });
  assert.equal(c.get("k", 200, 100), undefined, "too old");
  c.insert("gone", 100, undefined);
  assert.deepEqual(c.get("gone", 120, 100), { entry: undefined }, "absence is cached");
  assert.equal(c.get("other", 120, 100), undefined, "a miss is not an absence");

  // An entry that expired while cached reads as absent.
  c.insert("k", 100, e(2, 130));
  assert.deepEqual(c.get("k", 129, 100), { entry: e(2, 130) });
  assert.deepEqual(c.get("k", 130, 100), { entry: undefined });

  // Full: expired entries are dropped first, then everything.
  c.insert("third", 140, e(3));
  assert(c.entries.size <= 2);
  assert.deepEqual(c.get("third", 141, 100), { entry: e(3) });
  c.remove("third");
  assert.equal(c.get("third", 141, 100), undefined);
});

test("queue frames round-trip", () => {
  const messages: Message[] = [
    { id: 1, attempt: 2, enqueuedAt: 3, body: b("x") },
    { id: Number.MAX_SAFE_INTEGER, attempt: 0xffff_ffff, enqueuedAt: 0, body: new Uint8Array() },
  ];
  assert.deepEqual(decodeMessages(encodeMessages(messages)), messages);
  const items: Settlement[] = [
    { id: 1, attempt: 1, outcome: "ack" },
    { id: 2, attempt: 3, outcome: "retry" },
    { id: 3, attempt: 1, outcome: { retryAfterMs: 50 } },
  ];
  assert.deepEqual(decodeSettlements(encodeSettlements(items)), items);
  assert.deepEqual(decodeCounts(encodeCounts([4, 5])), [4n, 5n]);
  assert.deepEqual(decodeBodies(encodeBodies([b("a"), b("")])), [b("a"), new Uint8Array()]);
  const dead = [{ message: messages[0], failedAt: 9, reason: "attempts exhausted" }];
  assert.deepEqual(decodeDead(encodeDead(dead)), dead);
});

test("settlements encode as Rust does: u64::MAX means the queue's backoff", () => {
  const one = (outcome: Settlement["outcome"]) => hex(encodeSettlements([{ id: 5, attempt: 2, outcome }]));
  const head = "0000000000000001" + "0000000000000005" + "0000000000000002";
  assert.equal(one("ack"), head + "0000000000000000" + "0000000000000000");
  assert.equal(one("retry"), head + "0000000000000001" + "ffffffffffffffff");
  assert.equal(one({ retryAfterMs: 50 }), head + "0000000000000001" + "0000000000000032");
});

test("malformed frames are refused", () => {
  // A count promising more than the buffer holds.
  assert.equal(decodeMessages(new Uint8Array()), undefined);
  const one = encodeCounts([7]);
  one[7] = 2; // says two counts, holds one
  assert.equal(decodeCounts(one), undefined);
  // Trailing bytes are refused too.
  assert.equal(decodeCounts(Uint8Array.of(...encodeCounts([1]), 0)), undefined);
  // An unknown outcome kind.
  assert.equal(decodeSettlements(new Writer().u64(1).u64(1).u64(1).u64(9).u64(0).finish()), undefined);
  // An attempt past u32::MAX (Rust: `u32::try_from` fails).
  const attempt = new Writer().u64(1).u64(1).u64(0x1_0000_0000n).u64(0).bytes(b("x")).finish();
  assert.equal(decodeMessages(attempt), undefined);
  // A dead letter's reason must be UTF-8 (Rust: `String::from_utf8`).
  const reason = new Writer().u64(1).u64(1).u64(1).u64(0).bytes(b("x")).u64(9).bytes(Uint8Array.of(0xff)).finish();
  assert.equal(decodeDead(reason), undefined);
});
