// `actors.call` and `actors.callMany` against a fake host: what each sends,
// and how answers and call errors come back (ADR 0025: one entry per call,
// in input order, a CallError returned, not thrown).

import { test } from "node:test";
import assert from "node:assert/strict";
import { install, type Request } from "./host-stub";
import { actors, CallError } from "../src/actors";

const dec = new TextDecoder();

test("callMany answers one entry per call, in input order", () => {
  const seen: Array<[string, Request]> = [];
  install((address, request) => {
    seen.push([address, request]);
    if (address === "t/gone") throw { payload: { tag: "invalid-address", val: "no such actor class" } };
    if (address === "t/late") throw { payload: { tag: "timeout" } };
    return { status: 200, headers: [["x-who", address]], body: new TextEncoder().encode(`hi ${address}`) };
  });
  const results = actors.callMany([
    ["t/a", { method: "POST", path: "/x", headers: { "content-type": "text/plain" }, body: "one" }],
    ["t/gone"],
    ["t/b", { path: "/y" }],
    ["t/late"],
  ]);
  assert.equal(results.length, 4);
  const [a, gone, bee, late] = results;
  assert(!(a instanceof CallError));
  assert.equal(a.status, 200);
  assert.equal(a.text(), "hi t/a");
  assert.deepEqual(a.headers, { "x-who": "t/a" });
  assert(gone instanceof CallError);
  assert.equal(gone.kind, "invalid-address");
  assert.equal(gone.message, "no such actor class");
  assert(!(bee instanceof CallError));
  assert.equal(bee.text(), "hi t/b");
  assert(late instanceof CallError);
  assert.equal(late.kind, "timeout");

  // The requests as `call` would send them: defaults filled in.
  assert.deepEqual(
    seen.map(([address, r]) => [address, r.method, r.path, r.headers, dec.decode(r.body)]),
    [
      ["t/a", "POST", "/x", [["content-type", "text/plain"]], "one"],
      ["t/gone", "GET", "/", [], ""],
      ["t/b", "GET", "/y", [], ""],
      ["t/late", "GET", "/", [], ""],
    ],
  );
  assert.deepEqual(actors.callMany([]), []);
});

test("call throws the CallError callMany returns", () => {
  install(() => {
    throw { payload: { tag: "reentrant" } };
  });
  assert.throws(
    () => actors.call("t/a"),
    (e: unknown) => e instanceof CallError && e.kind === "reentrant" && e.message === "reentrant",
  );
  const [r] = actors.callMany([["t/a"]]);
  assert(r instanceof CallError && r.kind === "reentrant");
});
