// The structured error envelope: producing, classifying, the shared vectors
// the Rust SDK also checks, and the limits.

import { test } from "node:test";
import assert from "node:assert/strict";
import vectors from "./fixtures/problem-vectors.json";
import { CallError, CALL_ERROR_KINDS, isCallErrorKind } from "../src/errors";
import { MAX_PROBLEM_BYTES, classifyFailure, problem, problemResponse, type JsonValue } from "../src/problem";

const enc = new TextEncoder();

test("shared vectors: classification", () => {
  for (const v of vectors.decode) {
    const got = classifyFailure({ status: v.status, headers: { "content-type": v.content_type }, body: enc.encode(v.body) });
    const want = v.expect as { kind: string; code?: string; message?: string; details?: JsonValue; version?: number };
    assert.equal(got.kind, want.kind, v.name);
    if (got.kind === "problem") {
      assert.equal(got.problem.code, want.code, v.name);
      assert.equal(got.problem.message, want.message, v.name);
      assert.deepEqual(got.problem.details, want.details, v.name);
      assert.equal("details" in got.problem, "details" in want, v.name);
    }
    if (got.kind === "unsupported-version") assert.equal(got.version, want.version, v.name);
  }
});

test("shared vectors: production writes the same bytes", () => {
  for (const v of vectors.produce) {
    const r = problemResponse(v.status, { code: v.code, message: v.message, ...("details" in v ? { details: v.details as JsonValue } : {}) });
    assert.equal(r.status, v.status);
    assert.equal(r.body, v.body, v.name);
    assert.deepEqual(r.headers, { "content-type": vectors.content_type });
    // And what either SDK wrote reads back.
    const back = classifyFailure({ status: v.status, headers: r.headers as Record<string, string>, body: enc.encode(String(r.body)) });
    assert(back.kind === "problem" && back.problem.code === v.code && back.problem.message === v.message, v.name);
  }
});

test("producing refuses what a reader would refuse", () => {
  assert.throws(() => problem(200, "x", "m"), RangeError as never);
  assert.throws(() => problem(400, "bad code", "m"), RangeError as never);
  assert.throws(() => problem(400, "x", "m".repeat(513)), RangeError as never);
  assert.throws(() => problem(400, "x", "m", [[[[[[[[[1]]]]]]]]] as JsonValue), RangeError as never);
  assert.throws(() => problem(400, "x", "m", Number.NaN as JsonValue), RangeError as never);
  const wide: Record<string, JsonValue> = {};
  for (let i = 0; i < 65; i++) wide["k" + i] = 1;
  assert.throws(() => problem(400, "x", "m", wide), RangeError as never);
  assert.throws(() => problem(400, "x", "m", "d".repeat(MAX_PROBLEM_BYTES)), RangeError as never);
  assert.equal(problem(400, "x", "m").status, 400);
});

test("an oversized, non-UTF-8 or truncated envelope is malformed, not trusted", () => {
  const h = { "content-type": vectors.content_type };
  const big = enc.encode(`{"v":1,"code":"x","message":"${"m".repeat(MAX_PROBLEM_BYTES)}"}`);
  assert.equal(classifyFailure({ status: 400, headers: h, body: big }).kind, "malformed");
  assert.equal(classifyFailure({ status: 400, headers: h, body: new Uint8Array([0x7b, 0xff, 0xfe]) }).kind, "malformed");
  assert.equal(classifyFailure({ status: 400, headers: h, body: enc.encode('{"v":1,"code":"x","mess') }).kind, "malformed");
  assert.equal(classifyFailure({ status: 400, headers: h, body: new Uint8Array() }).kind, "malformed");
});

test("a plain failure keeps its status and a bounded text, or none when not text", () => {
  const long = classifyFailure({ status: 502, headers: {}, body: enc.encode("x".repeat(2000)) });
  assert(long.kind === "plain" && long.text?.length === 512);
  const bin = classifyFailure({ status: 500, headers: {}, body: new Uint8Array([0xff]) });
  assert(bin.kind === "plain" && bin.text === undefined);
});

test("call error kinds: known ones narrow, unknown ones pass through", () => {
  for (const k of CALL_ERROR_KINDS) assert.equal(new CallError(k, "m").known, k);
  const odd = new CallError("brand-new", "m");
  assert.equal(odd.known, undefined);
  assert.equal(odd.kind, "brand-new");
  assert.equal(isCallErrorKind("failed"), true);
  assert.equal(isCallErrorKind("nope"), false);
  assert(odd instanceof Error && odd.name === "CallError");
});

test("shared vectors: what the writer refuses", () => {
  for (const v of vectors.refuse) {
    const details = JSON.parse(v.details_json) as JsonValue;
    assert.throws(() => problem(400, v.code, v.message, details), RangeError as never, v.name);
  }
  assert.throws(() => problem(400, "x", "a\ud800b"), RangeError as never);
  assert.throws(() => problem(400, "x", "m", { ["a\udc00"]: 1 }), RangeError as never);
  assert.equal(problem(400, "x", "\ud83d\ude00").status, 400); // a pair is fine
});

test("hostile nesting is classified, never thrown", () => {
  const h = { "content-type": vectors.content_type };
  for (const n of [17, 100, 8000, 50000]) {
    const body = enc.encode(`{"v":1,"code":"x","message":"m","extra":${"[".repeat(n)}0${"]".repeat(n)}}`);
    assert.doesNotThrow(() => classifyFailure({ status: 400, headers: h, body }));
  }
  const body = enc.encode(`{"v":1,"code":"x","message":"m","extra":${"[".repeat(8000)}0${"]".repeat(8000)}}`);
  assert.equal(classifyFailure({ status: 400, headers: h, body }).kind, "malformed");
});
