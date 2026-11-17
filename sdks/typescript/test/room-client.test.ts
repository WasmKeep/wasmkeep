import { test } from "node:test";
import assert from "node:assert/strict";
import { Receiver, backoff, joinURL } from "../src/room-client";
test("client drops repeats, fetches gaps and preserves reconnect position", async () => {
  const delivered: number[] = [], queries: unknown[] = [];
  const r = new Receiver({ onMessage(p) { delivered.push(p.n); }, history: async p => {
    queries.push(p); return { head: { e: 1, n: 3 }, messages: [{ position: { e: 1, n: 2 }, at: 0, body: "2" }], more: false };
  } });
  await r.receive({ t: "ready", e: 1, n: 0 });
  await r.receive({ t: "m", e: 1, b: 0, n: 1, m: [{ a: 0, d: "1" }] });
  await r.receive({ t: "m", e: 1, b: 0, n: 1, m: [{ a: 0, d: "1" }] });
  await r.receive({ t: "m", e: 1, b: 0, n: 3, m: [{ a: 0, d: "3" }] });
  assert.deepEqual(delivered, [1, 2, 3]);
  assert.deepEqual(queries, [{ e: 1, n: 1 }]);
  assert.equal(joinURL("wss://example/join?c=a", r.last), "wss://example/join?c=a&e=1&n=3");
});
test("client announces lost and reset and bounds full jitter", async () => {
  const lost: number[][] = [], resets: unknown[] = [];
  const r = new Receiver({ onLost(a, b) { lost.push([a, b]); }, onReset(p) { resets.push(p); }, history: async () => ({ head: { e: 4, n: 20 }, reset: { e: 4, n: 20 }, messages: [{ position: { e: 4, n: 20 }, at: 0, body: "20" }], more: false }) });
  await r.receive({ t: "ready", e: 1, n: 5 });
  await r.receive({ t: "m", e: 2, b: 3, n: 4, m: [{ a: 0, d: "new" }] });
  assert.deepEqual(lost, [[4, 5]]);
  await r.receive({ t: "behind", e: 4, n: 20 });
  assert.deepEqual(resets, [{ e: 4, n: 20 }]);
  for (let k = 0; k < 100; k++) { assert.equal(backoff(k, () => 0), 0); assert(backoff(k, () => .999) < Math.min(30000, 500 * 2 ** k)); }
  assert.equal(backoff(0, () => .5, 4801), 1000);
  assert.equal(backoff(0, () => .5, 1012), 1000);
});
