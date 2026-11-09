// The pure parts of workflows: the status encoding (golden bytes shared
// with the Rust SDK's test), the backoff and settings, and URL splitting
// for http.fetch.

import { test } from "node:test";
import assert from "node:assert/strict";
import { decodeStatus, delay, encodeStatus, settings, type Status } from "../src/workflow-wire";
import { FetchError, splitUrl } from "../src/url";
import { hex } from "../src/wire";

test("the status encodes as the Rust SDK encodes it", () => {
  const s: Status = { state: "waiting", wakeAt: 42, waitingFor: ["approved", "paid"], positions: 3 };
  assert.equal(hex(encodeStatus(s)), "0000000777616974696e67000000000000002a0000000d617070726f7665640a70616964000000000000000000000000000000000000000000000003");
  assert.deepEqual(decodeStatus(encodeStatus(s)), { ...s, output: undefined, error: undefined });
  const done: Status = { state: "completed", waitingFor: [], output: new Uint8Array(), positions: 5 };
  const back = decodeStatus(encodeStatus(done));
  assert.equal(back?.state, "completed");
  assert.deepEqual(back?.output, new Uint8Array());
  assert.equal(decodeStatus(new TextEncoder().encode("junk")), undefined);
});

test("backoff doubles up to its cap", () => {
  const s = settings({ backoffBaseMs: 100, backoffMaxMs: 1_000 });
  assert.deepEqual([1, 2, 3, 4, 5, 6].map((a) => delay(s, a)), [100, 200, 400, 800, 1_000, 1_000]);
  assert.equal(delay(settings(), 1_000), 300_000);
  assert.equal(settings({ maxAttempts: 0 }).maxAttempts, 1);
});

test("URLs split into scheme, authority and path", () => {
  assert.deepEqual(splitUrl("https://api.example.com/v1/x?y=1"), {
    scheme: "HTTPS",
    authority: "api.example.com",
    path: "/v1/x?y=1",
  });
  assert.deepEqual(splitUrl("http://127.0.0.1:8080"), { scheme: "HTTP", authority: "127.0.0.1:8080", path: "/" });
  assert.deepEqual(splitUrl("HTTP://h?q=1#frag"), { scheme: "HTTP", authority: "h", path: "/?q=1" });
  for (const bad of ["ftp://h/x", "http:///x", "/relative", "nope"]) {
    assert.throws(() => splitUrl(bad), FetchError, bad);
  }
});
