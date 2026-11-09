// The kv and queue clients against fake actors: the exact requests they
// send (address, method, path, headers, body) and how they read answers,
// statuses and failed calls, as the Rust clients do.

import { test } from "node:test";
import assert from "node:assert/strict";
import { install, type Request, type Response } from "./host-stub";
import * as kv from "../src/kv";
import * as queue from "../src/queue";
import {
  Writer,
  decodeSettlements,
  encodeCounts,
  encodeDead,
  encodeEntry,
  encodeKeys,
  encodeMessages,
  hex,
} from "../src/wire";
import type { ActorRequest } from "../src/index";

const enc = new TextEncoder();
const b = (s: string) => enc.encode(s);

/** Records every call and answers with `answer`. */
function host(answer: (address: string, r: Request) => Response | { fail: { tag: string; val?: string } }) {
  const calls: { address: string; request: Request }[] = [];
  install((address, request) => {
    calls.push({ address, request: { ...request, body: request.body.slice() } });
    const r = answer(address, request);
    if ("fail" in r) throw { payload: r.fail };
    return r;
  });
  return calls;
}

const ok = (body: Uint8Array): Response => ({ status: 200, headers: [], body });
const status = (s: number, text = ""): Response => ({ status: s, headers: [], body: b(text) });
const u64 = (v: number) => new Writer().u64(v).finish();

function kvError(kind: string, message?: string, s?: number) {
  return (e: unknown) =>
    e instanceof kv.KvError &&
    e.kind === kind &&
    (message === undefined || e.message === message) &&
    (s === undefined || e.status === s);
}

function queueError(kind: string, message?: string, s?: number) {
  return (e: unknown) =>
    e instanceof queue.QueueError &&
    e.kind === kind &&
    (message === undefined || e.message === message) &&
    (s === undefined || e.status === s);
}

test("put, putWith and compareAndSet send what the Rust client sends", () => {
  const calls = host(() => ok(u64(7)));
  const store = new kv.Client("kv", 8);
  assert.equal(store.put("user:42", "Ada"), 7);
  assert.deepEqual(calls[0], {
    address: "kv/shard-2", // shard_for("user:42", 8) in Rust
    request: {
      method: "PUT",
      path: `/wasmkeep-kv/v1/k/${hex(b("user:42"))}`,
      headers: [
        ["x-wasmkeep-kv-expect", "any"],
        ["x-wasmkeep-kv-durable", "1"],
      ],
      body: b("Ada"),
    },
  });
  store.putWith("user:42", Uint8Array.of(0, 1), { expect: { version: 5 }, ttlMs: 60_000, durable: false });
  assert.deepEqual(calls[1].request.headers, [
    ["x-wasmkeep-kv-expect", "5"],
    ["x-wasmkeep-kv-durable", "0"],
    ["x-wasmkeep-kv-ttl-ms", "60000"],
  ]);
  assert.deepEqual(calls[1].request.body, Uint8Array.of(0, 1));
  store.compareAndSet("user:42", 6, "x");
  store.compareAndSet("user:42", undefined, "x");
  assert.equal(calls[2].request.headers[0][1], "6");
  assert.equal(calls[3].request.headers[0][1], "absent");
});

test("kv answers: conflict, refusal, malformed reply, failed call, long key", () => {
  host(() => status(409, "conflict\n"));
  const store = new kv.Client("kv", 4);
  assert.throws(() => store.put("k", "v"), kvError("conflict", "conflict: the key changed"));
  host(() => status(500, "storage: disk full\n"));
  assert.throws(() => store.get("k"), kvError("refused", "shard answered 500: storage: disk full", 500));
  host(() => ok(new Uint8Array(3)));
  assert.throws(() => store.get("k"), kvError("refused", "shard answered 200: malformed reply from shard", 200));
  host(() => ok(new Uint8Array()));
  assert.throws(() => store.put("k", "v"), kvError("refused", undefined, 200));
  const address = `kv/shard-${kv.shardFor("k", 4)}`;
  host(() => ({ fail: { tag: "timeout" } }));
  assert.throws(() => store.put("k", "v"), kvError("call", `call to ${address} timed out`));
  host(() => ({ fail: { tag: "failed", val: "trapped" } }));
  assert.throws(() => store.get("k"), kvError("call", `call to ${address} failed: trapped`));
  host(() => ({ fail: { tag: "reentrant" } }));
  assert.throws(
    () => store.delete("k"),
    kvError("call", `call to ${address} would re-enter an actor already in this call chain`),
  );
  host(() => ({ fail: { tag: "invalid-address", val: "no class kv" } }));
  assert.throws(() => store.get("k"), kvError("call", `invalid address ${address}: no class kv`));
  host(() => ({ fail: { tag: "too-large" } }));
  assert.throws(() => store.put("k", "v"), kvError("call", `call to ${address}: body too large`));

  const calls = host(() => ok(u64(1)));
  assert.throws(() => store.put("k".repeat(1025), "v"), kvError("key-too-long", "key longer than 1024 bytes"));
  assert.equal(store.put("k".repeat(1024), "v"), 1, "1024 bytes is allowed");
  // The limit is on UTF-8 bytes: 513 two-byte characters are too long.
  assert.throws(() => store.get("é".repeat(513)), kvError("key-too-long"));
  assert.equal(calls.length, 1);
});

test("get decodes an entry, 404 is absent", () => {
  const calls = host(() => ok(encodeEntry({ value: b("Ada"), version: 3, expiresAt: 99 })));
  const store = new kv.Client("kv", 1);
  assert.deepEqual(store.get("user:1"), { value: b("Ada"), version: 3, expiresAt: 99 });
  assert.deepEqual(calls[0], {
    address: "kv/shard-0",
    request: { method: "GET", path: `/wasmkeep-kv/v1/k/${hex(b("user:1"))}`, headers: [], body: new Uint8Array() },
  });
  host(() => status(404, "no such key\n"));
  assert.equal(store.getFresh("user:1"), undefined);
});

test("delete and deleteIf", () => {
  const calls = host(() => ok(u64(1)));
  const store = new kv.Client("kv", 1);
  assert.equal(store.delete("k"), true);
  assert.deepEqual(calls[0].request, {
    method: "DELETE",
    path: "/wasmkeep-kv/v1/k/6b",
    headers: [["x-wasmkeep-kv-expect", "any"]],
    body: new Uint8Array(),
  });
  host(() => ok(u64(0)));
  assert.equal(store.delete("k"), false);
  const cond = host(() => status(409));
  assert.throws(() => store.deleteIf("k", 4), kvError("conflict"));
  assert.deepEqual(cond[0].request.headers, [["x-wasmkeep-kv-expect", "4"]]);
});

test("list asks every shard and merges the pages", () => {
  const pages: Record<string, Uint8Array[]> = {
    "kv/shard-0": [b("user:3")],
    "kv/shard-1": [b("user:1"), b("user:4")],
    "kv/shard-2": [b("user:2")],
  };
  const calls = host((address) => ok(encodeKeys(pages[address])));
  const store = new kv.Client("kv", 3);
  assert.deepEqual(store.list("user:", "user:0", 3), [b("user:1"), b("user:2"), b("user:3")]);
  assert.deepEqual(
    calls.map((c) => [c.address, c.request.method, c.request.path]),
    ["kv/shard-0", "kv/shard-1", "kv/shard-2"].map((a) => [
      a,
      "GET",
      `/wasmkeep-kv/v1/list?prefix=${hex(b("user:"))}&limit=3&after=${hex(b("user:0"))}`,
    ]),
  );
  host(() => ok(encodeKeys([])));
  assert.equal(calls.length, 3);
  host(() => status(409, "conflict"));
  assert.throws(() => store.list("", undefined, 10), kvError("refused", undefined, 409), "not a conflict");
});

test("the read cache serves get and follows this client's writes", () => {
  let value = "one";
  let version = 1;
  const calls = host((_a, r) => {
    if (r.method === "GET") return ok(encodeEntry({ value: b(value), version }));
    if (r.method === "PUT") return ok(u64(++version));
    return ok(u64(1));
  });
  // A class name of its own: the cache is per instance, shared by clients.
  const cached = new kv.Client("cache-test", 2).withReadCache(60_000);
  const fresh = new kv.Client("cache-test", 2);
  const dec = (e?: kv.Entry) => e && new TextDecoder().decode(e.value);
  assert.equal(dec(cached.get("flag")), "one");
  value = "two"; // another actor's write
  assert.equal(dec(cached.get("flag")), "one", "served from the cache");
  assert.equal(dec(fresh.get("flag")), "two", "a client without a cache asks");
  assert.equal(dec(cached.getFresh("flag")), "two");
  value = "three";
  assert.equal(dec(cached.get("flag")), "two", "getFresh refreshed the cache");
  assert.equal(calls.length, 3);

  // Its own writes go through the cache: a put, then a delete.
  cached.put("flag", "mine");
  assert.deepEqual(cached.get("flag"), { value: b("mine"), version });
  cached.delete("flag");
  assert.equal(cached.get("flag"), undefined, "a delete caches the absence");
  const before = calls.length;
  // A conflict forgets the key: the next get asks the shard.
  host(() => status(409));
  assert.throws(() => cached.compareAndSet("flag", 1, "x"), kvError("conflict"));
  const after = host(() => ok(encodeEntry({ value: b("theirs"), version: 9 })));
  assert.equal(dec(cached.get("flag")), "theirs");
  assert.equal(after.length, 1);
  assert(before > 0);
  // A ttl write caches its expiry.
  host(() => ok(u64(10)));
  const now = Date.now();
  cached.putWith("session", "s", { ttlMs: 5_000 });
  const hit = cached.get("session");
  assert(hit?.expiresAt !== undefined && hit.expiresAt >= now + 5_000 && hit.expiresAt <= Date.now() + 5_000);
});

test("queue send, sendDelayed and sendBatch", () => {
  const calls = host((_a, r) => {
    const n = Number(new DataView(r.body.buffer, r.body.byteOffset).getBigUint64(0));
    return ok(encodeCounts(Array.from({ length: n }, (_, i) => 10 + i)));
  });
  const jobs = new queue.Client("queue/jobs");
  assert.equal(jobs.send("resize 42"), 10);
  assert.deepEqual(calls[0], {
    address: "queue/jobs",
    request: {
      method: "POST",
      path: "/wasmkeep-queue/v1/send",
      headers: [["x-wasmkeep-queue-delay-ms", "0"]],
      body: new Writer().u64(1).bytes(b("resize 42")).finish(),
    },
  });
  assert.equal(jobs.sendDelayed("later", 3_600_000), 10);
  assert.deepEqual(calls[1].request.headers, [["x-wasmkeep-queue-delay-ms", "3600000"]]);
  assert.deepEqual(jobs.sendBatch(["a", Uint8Array.of(1), ""]), [10, 11, 12]);
  assert.deepEqual(calls[2].request.body, new Writer().u64(3).bytes(b("a")).bytes(Uint8Array.of(1)).bytes(b("")).finish());

  host(() => ok(encodeCounts([1])));
  assert.throws(() => jobs.sendBatch(["a", "b"]), queueError("refused", "queue answered 200: malformed reply from queue"));
  host(() => status(413, "more than 1000 messages\n"));
  assert.throws(() => jobs.send("x"), queueError("refused", "queue answered 413: more than 1000 messages", 413));
  host(() => ({ fail: { tag: "timeout" } }));
  assert.throws(() => jobs.send("x"), queueError("call", "call to queue/jobs timed out"));
});

test("queue lease, ack, retry and settle", () => {
  const leased = [{ id: 4, attempt: 1, enqueuedAt: 1000, body: b("hello") }];
  const calls = host((_a, r) => {
    if (r.path.includes("/lease")) return ok(encodeMessages(leased));
    const items = decodeSettlements(r.body) ?? [];
    return ok(encodeCounts(items.map((s) => (s.attempt === 1 ? 1 : 0))));
  });
  const inbox = new queue.Client("queue/inbox");
  assert.deepEqual(inbox.lease(10, 30_000), leased);
  assert.deepEqual(calls[0].request, {
    method: "POST",
    path: "/wasmkeep-queue/v1/lease?max=10&visibility_ms=30000",
    headers: [],
    body: new Uint8Array(),
  });
  assert.equal(inbox.ack(leased[0]), true);
  assert.equal(calls[1].request.path, "/wasmkeep-queue/v1/settle");
  assert.equal(hex(calls[1].request.body), "0000000000000001" + "0000000000000004" + "0000000000000001" + "00".repeat(16));
  assert.equal(inbox.ack({ id: 4, attempt: 2 }), false, "a stale lease");
  assert.equal(inbox.retry(leased[0]), true);
  assert.deepEqual(decodeSettlements(calls[3].request.body), [{ id: 4, attempt: 1, outcome: "retry" }]);
  inbox.retry(leased[0], 250);
  assert.deepEqual(decodeSettlements(calls[4].request.body), [{ id: 4, attempt: 1, outcome: { retryAfterMs: 250 } }]);
  assert.deepEqual(
    inbox.settle([
      { id: 1, attempt: 1, outcome: "ack" },
      { id: 2, attempt: 2, outcome: "retry" },
    ]),
    [true, false],
  );
});

test("queue stats and dead letters", () => {
  const dead = [{ message: { id: 2, attempt: 3, enqueuedAt: 5, body: b("fail") }, failedAt: 9, reason: "attempts exhausted" }];
  const calls = host((_a, r) => (r.path.endsWith("/stats") ? ok(encodeCounts([1, 2, 3, 4])) : ok(encodeDead(dead))));
  const q = new queue.Client("queue/jobs");
  assert.deepEqual(q.stats(), { ready: 1, delayed: 2, inFlight: 3, dead: 4 });
  assert.deepEqual(q.deadLetters(100), dead);
  assert.deepEqual(
    calls.map((c) => [c.request.method, c.request.path]),
    [
      ["GET", "/wasmkeep-queue/v1/stats"],
      ["GET", "/wasmkeep-queue/v1/dead?limit=100"],
    ],
  );
  host(() => ok(encodeCounts([1, 2, 3])));
  assert.throws(() => q.stats(), queueError("refused", undefined, 200));
});

function request(method: string, path: string, headers: Record<string, string>, body: Uint8Array): ActorRequest {
  return {
    method,
    path,
    headers,
    bytes: () => body,
    text: () => new TextDecoder().decode(body),
    json: <T>() => JSON.parse(new TextDecoder().decode(body)) as T,
  };
}

test("handleDelivery answers one outcome per message", () => {
  const batch = [
    { id: 1, attempt: 1, enqueuedAt: 10, body: b("resize 42") },
    { id: 2, attempt: 3, enqueuedAt: 11, body: b("fail") },
  ];
  const deliver = request("POST", queue.DELIVER_PATH, { "x-wasmkeep-caller": "default/queue/jobs" }, encodeMessages(batch));
  assert(queue.isDelivery(deliver));
  assert(!queue.isDelivery(request("GET", queue.DELIVER_PATH, {}, new Uint8Array())));
  assert(!queue.isDelivery(request("POST", `${queue.DELIVER_PATH}?x=1`, {}, new Uint8Array())));
  const seen: string[] = [];
  const r = queue.handleDelivery(deliver, (from, m) => {
    seen.push(`${from} ${m.id}`);
    return new TextDecoder().decode(m.body).startsWith("fail") ? "retry" : "ack";
  });
  assert.deepEqual(seen, ["default/queue/jobs 1", "default/queue/jobs 2"]);
  assert(typeof r === "object" && !(r instanceof Uint8Array));
  assert.equal(r.status, 200);
  assert.deepEqual(r.headers, { "content-type": "application/octet-stream" });
  assert.deepEqual(decodeSettlements(r.body as Uint8Array), [
    { id: 1, attempt: 1, outcome: "ack" },
    { id: 2, attempt: 3, outcome: "retry" },
  ]);

  // The caller header is looked up case-insensitively.
  const mixed = request("POST", queue.DELIVER_PATH, { "X-WasmKeep-Caller": "default/queue/q" }, encodeMessages([]));
  assert.equal((queue.handleDelivery(mixed, () => "ack") as { status: number }).status, 200);
  const outside = request("POST", queue.DELIVER_PATH, {}, encodeMessages(batch));
  assert.deepEqual(queue.handleDelivery(outside, () => "ack"), {
    status: 403,
    headers: { "content-type": "text/plain" },
    body: "deliveries come from queue actors\n",
  });
  const garbage = request("POST", queue.DELIVER_PATH, { "x-wasmkeep-caller": "q" }, Uint8Array.of(1, 2, 3));
  assert.deepEqual(queue.handleDelivery(garbage, () => "ack"), {
    status: 400,
    headers: { "content-type": "text/plain" },
    body: "malformed delivery\n",
  });
});
