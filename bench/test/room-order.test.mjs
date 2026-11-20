import { test } from 'node:test';
import assert from 'node:assert/strict';
import { deliveryOrder, historyCoverage } from '../room-order.mjs';
test('a number handed to the application twice is a repeat', () => {
  const o = deliveryOrder();
  assert.equal(o.message({ e: 1, n: 1 }), false);
  assert.equal(o.message({ e: 1, n: 2 }), false);
  assert.equal(o.message({ e: 1, n: 2 }), true);
  assert.equal(o.message({ e: 1, n: 1 }), true);
  // History rows keep their original epoch: an older epoch is still a repeat.
  assert.equal(o.message({ e: 0, n: 2 }), true);
  assert.equal(o.message({ e: 1, n: 3 }), false);
});
test('void numbers reused by the next epoch and a reset are not repeats', () => {
  const o = deliveryOrder();
  for (const n of [1, 2, 3]) o.message({ e: 1, n });
  o.lost(3, 3);
  assert.equal(o.message({ e: 2, n: 3 }), false);
  o.reset({ e: 3, n: 1 });
  assert.equal(o.message({ e: 3, n: 1 }), false);
  assert.equal(o.message({ e: 3, n: 1 }), true);
});
test('history coverage classifies discards by the highest number a page carried', () => {
  const h = historyCoverage();
  assert.equal(h.covers({ e: 1, n: 1 }), false);
  h.page([{ position: { e: 1, n: 4 } }, { position: { e: 1, n: 5 } }]);
  assert.equal(h.covers({ e: 1, n: 5 }), true);
  assert.equal(h.covers({ e: 1, n: 6 }), false);
});
