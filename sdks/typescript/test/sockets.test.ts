import { test } from "node:test";
import assert from "node:assert/strict";
import { trySend } from "../src/sockets";
import { outcome } from "./sockets-stub";
test("trySend preserves the WIT result distinction", () => {
  outcome("ok"); assert.equal(trySend(1n, "x"), "ok");
  outcome("backpressure"); assert.equal(trySend(1n, "x"), "backpressure");
  outcome("unknown-connection"); assert.equal(trySend(1n, "x"), "gone");
  outcome("unexpected"); assert.throws(() => trySend(1n, "x"));
});
