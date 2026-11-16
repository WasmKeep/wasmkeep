// The typed client over a fake transport: successes, every failure kind,
// legacy answers, and batching (order, the limit, partial failures).

import { test } from "node:test";
import assert from "node:assert/strict";
import * as codec from "../src/codec";
import { CallError } from "../src/errors";
import { defineContract, defineOperation, urlPath } from "../src/contracts";
import { Client, MAX_BATCH, type Transport } from "../src/client";
import { problem } from "../src/problem";
import type { WireRequest } from "../src/contracts";

const enc = new TextEncoder();
type Answer = { status: number; headers: Record<string, string>; body: Uint8Array };
const json = (status: number, v: unknown): Answer => ({ status, headers: { "content-type": "application/json" }, body: enc.encode(JSON.stringify(v)) });
const fromProblem = (r: ReturnType<typeof problem>): Answer => ({
  status: r.status ?? 500,
  headers: r.headers ?? {},
  body: enc.encode(String(r.body)),
});

const addItem = defineOperation({
  name: "cart.addItem",
  method: "POST",
  path: (i: { sku: string }) => urlPath`/items/${i.sku}`,
  input: codec.object({ sku: codec.string(), qty: codec.integer() }),
  output: codec.object({ count: codec.integer(), note: codec.optional(codec.nullable(codec.string())) }),
  errors: { "cart.out_of_stock": codec.object({ left: codec.integer() }), "cart.closed": codec.none() },
});
const ping = defineOperation({ name: "ping", path: "/ping" });
const contract = defineContract({ addItem, ping });

function client(handle: (address: string, r: WireRequest) => Answer, many?: Transport["callMany"]) {
  const calls: Array<[string, WireRequest]> = [];
  const transport: Transport = {
    call(address, r) {
      calls.push([address, r]);
      return handle(address, r);
    },
    callMany:
      many ??
      ((cs) =>
        cs.map(([a, r]) => {
          try {
            return handle(a, r);
          } catch (e) {
            if (e instanceof CallError) return e;
            throw e;
          }
        })),
  };
  return { c: new Client(transport), calls };
}

test("a success is decoded and the request carries the encoded path and body", () => {
  const { c, calls } = client(() => json(200, { count: 3, extra: 1 }));
  const r = c.call("cart/alice", addItem, { sku: "a/b", qty: 2 });
  assert.deepEqual(r, { ok: true, status: 200, value: { count: 3 } });
  assert.equal(calls[0][0], "cart/alice");
  assert.equal(calls[0][1].path, "/items/a%2Fb");
  assert.equal(new TextDecoder().decode(calls[0][1].body), '{"sku":"a/b","qty":2}');
});

test("HTTP 200 with the wrong shape is invalid-response, not a value", () => {
  for (const body of [{ count: "3" }, { count: 1.5 }, [], null, { note: null }]) {
    const { c } = client(() => json(200, body));
    const r = c.call("a/b", addItem, { sku: "s", qty: 1 });
    assert(!r.ok && r.failure.kind === "invalid-response", JSON.stringify(body));
  }
  const bad = (a: Answer) => {
    const { c } = client(() => a);
    const r = c.call("a/b", addItem, { sku: "s", qty: 1 });
    assert(!r.ok && r.failure.kind === "invalid-response");
  };
  bad({ status: 200, headers: {}, body: enc.encode("not json") });
  bad({ status: 200, headers: {}, body: new Uint8Array([0xff]) });
  bad({ status: 200, headers: {}, body: new Uint8Array() });
});

test("an empty success decodes as no value; an oversized one is refused", () => {
  const { c } = client(() => ({ status: 204, headers: {}, body: new Uint8Array() }));
  assert.deepEqual(c.call("a/b", ping), { ok: true, status: 204, value: undefined });
  const small = defineOperation({ name: "s", path: "/", output: codec.unknown(), maxResponseBytes: 8 });
  const big = client(() => json(200, "123456789")).c.call("a/b", small);
  assert(!big.ok && big.failure.kind === "invalid-response");
});

test("input that is not valid sends nothing", () => {
  const { c, calls } = client(() => json(200, {}));
  const r = c.call("a/b", addItem, { sku: "s", qty: "x" } as never);
  assert(!r.ok && r.failure.kind === "invalid-request");
  assert.equal(calls.length, 0);
});

test("declared errors come back typed; undeclared and odd ones stay explicit", () => {
  const answer = (a: Answer) => client(() => a).c.call("a/b", addItem, { sku: "s", qty: 1 });

  let r = answer(fromProblem(problem(409, "cart.out_of_stock", "Out of stock", { left: 2 })));
  assert(!r.ok && r.failure.kind === "declared" && r.failure.code === "cart.out_of_stock");
  if (!r.ok && r.failure.kind === "declared" && r.failure.code === "cart.out_of_stock") assert.equal(r.failure.details.left, 2);

  r = answer(fromProblem(problem(409, "cart.closed", "Closed")));
  assert(!r.ok && r.failure.kind === "declared" && r.failure.code === "cart.closed");

  r = answer(fromProblem(problem(409, "cart.new_thing", "New", { a: 1 })));
  assert(!r.ok && r.failure.kind === "problem" && r.failure.problem.code === "cart.new_thing");

  r = answer(fromProblem(problem(409, "cart.out_of_stock", "Out", { left: "many" })));
  assert(!r.ok && r.failure.kind === "invalid-details");

  r = answer(fromProblem(problem(409, "cart.closed", "Closed", { surprise: 1 })));
  assert(!r.ok && r.failure.kind === "invalid-details");

  const h = { "content-type": "application/vnd.wasmkeep.error+json" };
  r = answer({ status: 400, headers: h, body: enc.encode('{"v":7,"code":"x","message":"m"}') });
  assert(!r.ok && r.failure.kind === "unsupported-envelope" && r.failure.version === 7);
  r = answer({ status: 400, headers: h, body: enc.encode("garbage") });
  assert(!r.ok && r.failure.kind === "malformed-envelope");
});

test("a prototype-named code is not declared", () => {
  const r = client(() => fromProblem(problem(400, "constructor", "m"))).c.call("a/b", addItem, { sku: "s", qty: 1 });
  assert(!r.ok && r.failure.kind === "problem");
});

test("legacy answers: plain text and bare JSON failures are http failures", () => {
  const t = client(() => ({ status: 500, headers: { "content-type": "text/plain" }, body: enc.encode("internal error\n") })).c.call("a/b", ping);
  assert(!t.ok && t.failure.kind === "http" && t.failure.status === 500 && t.failure.text === "internal error\n");
  const j = client(() => json(404, { error: "nope" })).c.call("a/b", ping);
  assert(!j.ok && j.failure.kind === "http" && j.failure.status === 404);
  const redirect = client(() => ({ status: 302, headers: {}, body: new Uint8Array() })).c.call("a/b", ping);
  assert(!redirect.ok && redirect.failure.kind === "http");
});

test("transport failures are their own kind, whatever the status code would suggest", () => {
  for (const kind of ["timeout", "failed", "reentrant", "too-large", "invalid-address", "future-kind"]) {
    const { c } = client(() => {
      throw new CallError(kind, kind);
    });
    const r = c.call("a/b", ping);
    assert(!r.ok && r.failure.kind === "transport" && r.failure.error.kind === kind);
  }
  // A 503 answer is the callee's answer, not a transport failure, and is not a retry hint.
  const r = client(() => ({ status: 503, headers: {}, body: enc.encode("busy") })).c.call("a/b", ping);
  assert(!r.ok && r.failure.kind === "http" && r.failure.status === 503);
  // Anything that is not a CallError is a bug and propagates.
  assert.throws(() => client(() => { throw new Error("boom"); }).c.call("a/b", ping), (e: unknown) => e instanceof Error && e.message === "boom");
});

test("bind gives the contract's methods for one actor", () => {
  const { c, calls } = client(() => json(200, { count: 1 }));
  const cart = c.bind("cart/alice", contract);
  assert(cart.addItem({ sku: "s", qty: 1 }).ok);
  assert.equal(calls[0][0], "cart/alice");
  const p = client(() => ({ status: 200, headers: {}, body: new Uint8Array() })).c.bind("x/y", contract);
  assert(p.ping().ok);
});

test("batch keeps input order, and each entry fails or succeeds on its own", () => {
  const seen: string[] = [];
  const { c } = client((address, r) => {
    seen.push(address + r.path);
    if (address === "gone/x") throw new CallError("invalid-address", "no such class");
    if (address === "late/x") throw new CallError("timeout", "timeout");
    if (address === "full/x") return fromProblem(problem(409, "cart.out_of_stock", "m", { left: 0 }));
    return json(200, { count: seen.length });
  });
  const r = c.batch([
    ["a/1", addItem, { sku: "s", qty: 1 }],
    ["gone/x", ping],
    ["full/x", addItem, { sku: "s", qty: 1 }],
    ["a/2", addItem, { sku: "bad", qty: 0.5 }],
    ["late/x", ping],
    ["a/3", addItem, { sku: "s", qty: 1 }],
  ] as const);
  assert.equal(r.length, 6);
  assert(r[0].ok);
  assert(!r[1].ok && r[1].failure.kind === "transport" && r[1].failure.error.kind === "invalid-address");
  assert(!r[2].ok && r[2].failure.kind === "declared");
  assert(!r[3].ok && r[3].failure.kind === "invalid-request");
  assert(!r[4].ok && r[4].failure.kind === "transport" && r[4].failure.error.kind === "timeout");
  assert(r[5].ok);
  // The invalid entry was never sent.
  assert.deepEqual(seen, ["a/1/items/s", "gone/x/ping", "full/x/items/s", "late/x/ping", "a/3/items/s"]);
});

test("batch: the limit is the host's, an empty batch calls nothing", () => {
  let many = 0;
  const { c } = client(() => json(200, { count: 1 }), (cs) => {
    many++;
    return cs.map(() => ({ status: 200, headers: {}, body: enc.encode('{"count":1}') }));
  });
  assert.deepEqual(c.batch([] as const), []);
  assert.equal(many, 0);
  const ok = Array.from({ length: MAX_BATCH }, () => ["a/b", ping] as const);
  assert.equal(c.batch(ok).length, MAX_BATCH);
  assert.equal(many, 1);
  const tooMany = Array.from({ length: MAX_BATCH + 1 }, () => ["a/b", ping] as const);
  const r = c.batch(tooMany);
  assert.equal(r.length, MAX_BATCH + 1);
  assert(r.every((x) => !x.ok && x.failure.kind === "transport" && x.failure.error.kind === "failed"));
  assert.equal(many, 1);
  assert.equal(MAX_BATCH, 16);
});

test("a hostile envelope in one batch entry does not cost the others their results", () => {
  const h = { "content-type": "application/vnd.wasmkeep.error+json" };
  const hostile = `{"v":1,"code":"x","message":"m","extra":${"[".repeat(8000)}0${"]".repeat(8000)}}`;
  const { c } = client((address) =>
    address === "evil/x" ? { status: 400, headers: h, body: enc.encode(hostile) } : json(200, { count: 1 }),
  );
  const r = c.batch([
    ["a/1", addItem, { sku: "s", qty: 1 }],
    ["evil/x", addItem, { sku: "s", qty: 1 }],
    ["a/2", addItem, { sku: "s", qty: 1 }],
  ] as const);
  assert(r[0].ok && r[2].ok);
  assert(!r[1].ok && r[1].failure.kind === "malformed-envelope");
});

test("an interpretation failure stays in its entry", () => {
  const poison = defineOperation({
    name: "poison",
    path: "/",
    output: { parse: () => { throw new RangeError("deep"); } },
  });
  const r = client(() => json(200, {})).c.batch([["a/1", ping], ["a/2", poison], ["a/3", ping]] as const);
  assert(!r[1].ok && r[1].failure.kind === "invalid-response");
  assert.equal(r.length, 3);
});

test("a third-party input codec that throws makes that entry invalid-request, nothing else", () => {
  const zodLike = { parse(v: unknown): { n: number } { if (typeof (v as { n?: unknown })?.n !== "number") throw new Error("Expected number"); return v as { n: number }; } };
  const op = defineOperation({ name: "z", method: "POST", path: "/", input: zodLike, output: codec.unknown() });
  const sentTo: string[] = [];
  const { c } = client((a) => { sentTo.push(a); return json(200, 1); });
  const single = c.call("a/0", op, { n: "x" } as never);
  assert(!single.ok && single.failure.kind === "invalid-request" && single.failure.reason.includes("Expected number"));
  const r = c.batch([["a/1", op, { n: 1 }], ["a/2", op, { n: "x" } as never], ["a/3", op, { n: 3 }]] as const);
  assert(r[0].ok && r[2].ok);
  assert(!r[1].ok && r[1].failure.kind === "invalid-request");
  assert.deepEqual(sentTo, ["a/1", "a/3"]);
  // A path or body function that throws is the same.
  const badPath = defineOperation({ name: "p", path: () => { throw new Error("no path"); } });
  const bp = c.call("a/4", badPath);
  assert(!bp.ok && bp.failure.kind === "invalid-request");
  const badBody = defineOperation({ name: "b", method: "POST", path: "/", body: () => { throw new Error("no body"); } });
  const bb = c.call("a/5", badBody);
  assert(!bb.ok && bb.failure.kind === "invalid-request");
});
