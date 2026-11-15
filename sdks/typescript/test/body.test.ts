// The request body (./body.ts): read once, and with a limit, never more
// than `limit + 1` bytes of it, whatever the client declared or sends in
// chunks; the stream is released either way.

import { test } from "node:test";
import assert from "node:assert/strict";
import { Body, incomingSource, type BodySource, type FutureTrailersLike } from "../src/body";

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

// The incoming body's stream (incomingSource): only `closed` is the end,
// and only once the trailers say it ended well.

/** A `wasi:io` error resource. */
const ioError = (text: string) => ({ toDebugString: () => text });
/** A stream error as jco throws it: an Error carrying the value, or the
 * bare value (the engines differ). */
const thrown = (tag: string, val?: unknown, wrapped = true) => {
  const payload = val === undefined ? { tag } : { tag, val };
  return wrapped ? Object.assign(new Error("see payload"), { payload }) : payload;
};

type Ended = ReturnType<FutureTrailersLike["get"]>;

/** A request body of `chunks`, then `end` thrown; trailers `trailers`,
 * ready only after a wait when `late`. Counts what was released. */
function incoming(chunks: number[], end: unknown, trailers: Ended = { tag: "ok", val: { tag: "ok", val: undefined } }, late = false) {
  const left = [...chunks];
  const seen = { finished: 0, streamDropped: 0, trailersDropped: 0, waits: 0 };
  let ready = !late;
  const handle = (count: keyof typeof seen) => ({ [Symbol.dispose]: () => void seen[count]++ });
  const stream = {
    ...handle("streamDropped"),
    blockingRead(len: bigint): Uint8Array {
      assert.equal(seen.streamDropped, 0, "read after the stream was dropped");
      const next = left.shift();
      if (next === undefined) throw end;
      const n = Math.min(next, Number(len));
      if (n < next) left.unshift(next - n);
      return new Uint8Array(n).fill(98);
    },
  };
  const future = {
    ...handle("trailersDropped"),
    subscribe: () => ({}),
    get: () => (ready ? trailers : undefined),
  };
  const source = () =>
    incomingSource({
      stream,
      finish: () => {
        assert.equal(seen.streamDropped, 1, "the stream is dropped before the body is finished");
        seen.finished++;
        return future;
      },
      wait: () => {
        seen.waits++;
        ready = true;
      },
      codeOf: (e: { toDebugString(): string }) =>
        e.toDebugString() === "too big" ? { tag: "HTTP-request-body-size", val: 16777216n } : undefined,
    });
  return { body: new Body(source), seen };
}

test("a body that ends with `closed` and good trailers is whole, either error shape", () => {
  for (const wrapped of [true, false]) {
    const { body, seen } = incoming([70000, 5], thrown("closed", undefined, wrapped));
    assert.equal(body.read().length, 70005);
    assert.deepEqual(seen, { finished: 1, streamDropped: 1, trailersDropped: 1, waits: 0 });
  }
  // Trailers not ready yet: waited for.
  const { body, seen } = incoming([3], thrown("closed"), undefined, true);
  assert.equal(body.read(10)?.length, 3);
  assert.equal(seen.waits, 1);
});

test("a body that breaks after a complete-looking payload is an error, not its prefix", () => {
  for (const wrapped of [true, false]) {
    // The client sent what looks like a whole JSON document, then the
    // connection reset before the declared length.
    const { body, seen } = incoming([2], thrown("last-operation-failed", ioError("connection reset"), wrapped));
    assert.throws(() => body.read(), /the request body is incomplete: connection reset/);
    // Nothing kept: every later read fails too, with or without a limit.
    assert.throws(() => body.read(), /incomplete/);
    assert.throws(() => body.read(1000), /incomplete/);
    assert.deepEqual(seen, { finished: 1, streamDropped: 1, trailersDropped: 1, waits: 0 });
  }
});

test("a streamed body over the host's cap is an error, whatever the limit above it", () => {
  // The host stops a chunked body at 16 MiB and fails the next read.
  const cap = Array(256).fill(65536);
  const { body, seen } = incoming(cap, thrown("last-operation-failed", ioError("too big")));
  assert.throws(() => body.read(32 << 20), /incomplete: HTTP-request-body-size: "16777216"/);
  assert.throws(() => body.read(), /incomplete/);
  assert.equal(seen.finished, 1);
  // Without a limit, the same.
  const whole = incoming(cap, thrown("last-operation-failed", ioError("too big")));
  assert.throws(() => whole.body.read(), /HTTP-request-body-size/);
  // A limit under the cap stops first: too large, not an error.
  const under = incoming(cap, thrown("last-operation-failed", ioError("too big")));
  assert.equal(under.body.read(1000), undefined);
  assert.deepEqual(under.seen, { finished: 1, streamDropped: 1, trailersDropped: 1, waits: 0 });
});

test("a body whose trailers carry an error is an error", () => {
  const failed = incoming([10], thrown("closed"), { tag: "ok", val: { tag: "err", val: { tag: "HTTP-protocol-error" } } });
  assert.throws(() => failed.body.read(), /incomplete: HTTP-protocol-error/);
  assert.throws(() => failed.body.read(100), /incomplete/);
  assert.equal(failed.seen.trailersDropped, 1);
  const taken = incoming([10], thrown("closed"), { tag: "err", val: undefined });
  assert.throws(() => taken.body.read(), /trailers were already taken/);
});
