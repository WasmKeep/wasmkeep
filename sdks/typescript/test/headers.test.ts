// Headers are ordered (name, value) pairs: names compare without case, `get`
// is the first value, `getAll` every value in order, `append` adds, `set`
// replaces all, and Set-Cookie is never joined with a comma.

import { test } from "node:test";
import assert from "node:assert/strict";
import { install } from "./host-stub";
import { actors } from "../src/actors";
import { HeaderList, fieldsToPairs, fetchedRecord, responsePairs } from "../src/headers";
import { header as wireHeader } from "../src/wire";

const COOKIES: [string, string][] = [
  ["set-cookie", "a=1; Expires=Wed, 21 Oct 2026 07:28:00 GMT"],
  ["x-other", "o"],
  ["Set-Cookie", "b=2"],
];

test("get is the first value and getAll every value, in order, whatever the case", () => {
  const h = new HeaderList(COOKIES);
  assert.equal(h.get("SET-COOKIE"), "a=1; Expires=Wed, 21 Oct 2026 07:28:00 GMT");
  assert.deepEqual(h.getAll("set-Cookie"), ["a=1; Expires=Wed, 21 Oct 2026 07:28:00 GMT", "b=2"]);
  assert.equal(h.get("missing"), undefined);
  assert.deepEqual(h.getAll("missing"), []);
  assert.equal(h.has("X-OTHER"), true);
  assert.deepEqual(h.entries(), COOKIES);
});

test("append adds without replacing, set replaces all, delete removes all", () => {
  const h = new HeaderList([["a", "1"]]);
  h.append("A", "2").append("b", "x");
  assert.deepEqual(h.getAll("a"), ["1", "2"]);
  h.set("a", "3");
  assert.deepEqual(h.entries(), [
    ["a", "3"],
    ["b", "x"],
  ]);
  h.set("c", "y");
  assert.deepEqual(h.getAll("c"), ["y"]);
  h.delete("B");
  assert.equal(h.has("b"), false);
});

test("a record with array values is one pair per value; pairs keep their order", () => {
  assert.deepEqual(new HeaderList({ "set-cookie": ["a=1", "b=2"], x: "y" }).entries(), [
    ["set-cookie", "a=1"],
    ["set-cookie", "b=2"],
    ["x", "y"],
  ]);
  const src = new HeaderList([["z", "1"]]);
  assert.deepEqual(new HeaderList(src).entries(), [["z", "1"]]);
});

test("a response keeps repeated headers and does not add a second content-type", () => {
  assert.deepEqual(responsePairs({ "Content-Type": "application/json", "Set-Cookie": ["a=1", "b=2"] }), [
    ["content-type", "application/json"],
    ["set-cookie", "a=1"],
    ["set-cookie", "b=2"],
  ]);
  assert.deepEqual(responsePairs({ "Content-Type": "a/b" }, { "set-cookie": ["x=1"] }), [
    ["content-type", "a/b"],
    ["set-cookie", "x=1"],
  ]);
  assert.deepEqual(responsePairs(undefined), [["content-type", "text/plain; charset=utf-8"]]);
  assert.deepEqual(
    responsePairs(undefined, [
      ["Set-Cookie", "a=1"],
      ["set-cookie", "b=2"],
    ]),
    [
      ["content-type", "text/plain; charset=utf-8"],
      ["set-cookie", "a=1"],
      ["set-cookie", "b=2"],
    ],
  );
});

test("headers read from the host keep every pair; the record joins all but Set-Cookie", () => {
  const enc = new TextEncoder();
  const raw = COOKIES.map(([k, v]) => [k, enc.encode(v)] as [string, Uint8Array]);
  raw.push(["accept", enc.encode("a")], ["Accept", enc.encode("b")]);
  const pairs = fieldsToPairs(raw);
  assert.equal(pairs.length, 5);
  const rec = fetchedRecord(pairs);
  assert.equal(rec["accept"], "a, b");
  assert.equal(rec["set-cookie"], "a=1; Expires=Wed, 21 Oct 2026 07:28:00 GMT");
});

test("a call sends every header it is given and a response keeps every header it came with", () => {
  let sent: [string, string][] = [];
  install((_a, r) => {
    sent = r.headers;
    return { status: 200, headers: COOKIES, body: new Uint8Array() };
  });
  const r = actors.call("t/x", { headers: { "Set-Cookie": ["a=1", "b=2"], "X-One": "1" } });
  assert.deepEqual(sent, [
    ["Set-Cookie", "a=1"],
    ["Set-Cookie", "b=2"],
    ["X-One", "1"],
  ]);
  assert.deepEqual(r.fields.getAll("set-cookie"), ["a=1; Expires=Wed, 21 Oct 2026 07:28:00 GMT", "b=2"]);
  assert.deepEqual(r.headerList, COOKIES);
  // The record is as it was: lower-case-insensitive keys, the last value.
  assert.equal(r.headers["Set-Cookie"], "b=2");

  actors.call("t/x", {
    headers: [
      ["a", "1"],
      ["a", "2"],
    ],
  });
  assert.deepEqual(sent, [
    ["a", "1"],
    ["a", "2"],
  ]);
});

test("the wire helper looks a header up without case in a record too", () => {
  assert.equal(wireHeader({ "X-Wasmkeep-Caller": "c" }, "x-wasmkeep-caller"), "c");
});
