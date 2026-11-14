// The request body (./body.ts): read once, and with a limit, never more
// than `limit + 1` bytes of it, whatever the client declared or sends in
// chunks; the stream is released either way.

import { test } from "node:test";
import assert from "node:assert/strict";
import { Body, type BodySource } from "../src/body";

/** A body sent as `chunks`, recording what was asked of it. */
function source(chunks: number[]): { open: () => BodySource; asked: number[]; read: () => number; closed: () => number } {
  const asked: number[] = [];
  let read = 0;
  let closed = 0;
  let opened = 0;
  const left = [...chunks];
  return {
    open: () => {
      opened++;
      assert.equal(opened, 1, "opened once");
      return {
        read(max: number) {
          asked.push(max);
          const next = left.shift();
          if (next === undefined) return undefined;
          // A chunk is at most what is asked; the rest stays for the next read.
          const n = Math.min(next, max);
          if (n < next) left.unshift(next - n);
          read += n;
          return new Uint8Array(n).fill(97);
        },
        close: () => void closed++,
      };
    },
    asked,
    read: () => read,
    closed: () => closed,
  };
}

test("without a limit, the whole body, read once", () => {
  const s = source([70000, 5]);
  const body = new Body(s.open);
  assert.equal(body.read().length, 70005);
  assert.equal(body.read().length, 70005, "kept");
  assert.equal(s.closed(), 1);
});

test("within the limit, the body", () => {
  const s = source([400, 600]);
  const body = new Body(s.open);
  assert.equal(body.read(1000)?.length, 1000);
  assert.equal(s.closed(), 1);
  // Kept: a smaller limit says too large, no limit gives it.
  assert.equal(body.read(999), undefined);
  assert.equal(body.read()?.length, 1000);
});

test("past the limit, undefined after limit + 1 bytes at most", () => {
  // A chunked body of 16 MiB: read stops past the limit.
  const s = source(Array(256).fill(65536));
  const body = new Body(s.open);
  assert.equal(body.read(1000), undefined);
  assert.equal(s.read(), 1001);
  assert(s.asked.every((n) => n <= 1001), `${s.asked}`);
  assert.equal(s.closed(), 1, "released");
  // Known too large for any limit up to it; the rest was not read.
  assert.equal(body.read(10), undefined);
  assert.equal(body.read(1000), undefined);
  assert.throws(() => body.read(), /too large/);
  assert.throws(() => body.read(2000), /too large/);
  assert.equal(s.read(), 1001);
});

test("an empty body and a limit of 0", () => {
  assert.equal(new Body(source([]).open).read(0)?.length, 0);
  assert.equal(new Body(source([1]).open).read(0), undefined);
});
