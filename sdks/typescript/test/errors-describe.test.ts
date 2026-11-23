// A WIT error// value must be shown, and a response stream the caller closed must not trap.
import { test } from "node:test";
import assert from "node:assert/strict";
import { describeError } from "../src/errors";
import { errorMessage } from "../src/broadcast-core";
class ComponentError extends Error {
    constructor(readonly payload: unknown) { super(`${String(payload)} (see error.payload)`); }
}
test("a WIT error value shows its fields, not [object Object]", () => {
    const closed = describeError(new ComponentError({ tag: "closed" }));
    assert(/"tag":"closed"/.test(closed), closed);
    assert(!/object Object/.test(closed), closed);
    assert(/not durable.*"n":"1"/.test(describeError(new ComponentError({ kind: "internal", message: "not durable: still uploading", n: 1n }))));
    assert.equal(errorMessage(new ComponentError({ kind: "internal", message: "not durable: still uploading" })), "not durable: still uploading");
    assert.equal(describeError({ kind: "x" }), '{"kind":"x"}');
    assert.equal(describeError(new TypeError("bad")), "TypeError: bad");
    assert.equal(describeError("refused"), "refused");
});
import { writeBody } from "../src/response-body";
test("a response stream the caller closed ends the write quietly; other failures are thrown", () => {
    let writes = 0, finished = false;
    const logged: string[] = [], error = console.error;
    console.error = (m: string) => { logged.push(m); };
    try {
        writeBody({ blockingWriteAndFlush() { if (++writes === 2) throw new ComponentError({ tag: "closed" }); } }, new Uint8Array(20000), () => { finished = true; });
        assert.equal(writes, 2);
        assert.equal(finished, true);
        writeBody({ blockingWriteAndFlush() { throw new ComponentError({ tag: "closed" }); } }, new Uint8Array(10), () => { throw new ComponentError({ tag: "closed" }); });
        // Not a departure: thrown, after the stream is released.
        let released = false;
        const stream = { blockingWriteAndFlush() { throw new ComponentError({ tag: "last-operation-failed", val: {} }); }, [Symbol.dispose]() { released = true; } };
        assert.throws(() => writeBody(stream, new Uint8Array(10), () => { }), ComponentError);
        assert(released);
        assert.throws(() => writeBody({ blockingWriteAndFlush() { } }, new Uint8Array(10), () => { throw new ComponentError({ tag: "HTTP-response-body-size", val: 10n }); }), ComponentError);
    }
    finally {
        console.error = error;
    }
    assert.equal(logged.length, 2);
    assert(logged.every(m => m.includes('"tag":"closed"')), logged.join("\n"));
});
import { describeStreamError } from "../src/stream-error";
test("a broken stream's error is released once described", () => {
    let dropped = 0;
    const error = { toDebugString: () => "connection reset", [Symbol.dispose]() { dropped += 1; } };
    assert.equal(describeStreamError(new ComponentError({ tag: "last-operation-failed", val: error })), "connection reset");
    assert.equal(dropped, 1);
    // With its HTTP error code, when the host gives one.
    const coded = describeStreamError(new ComponentError({ tag: "last-operation-failed", val: error }), () => ({ tag: "connection-terminated" }));
    assert.equal(coded, "connection-terminated");
    assert.equal(dropped, 2);
    assert.equal(describeStreamError(new ComponentError({ tag: "closed" })), "closed");
});
