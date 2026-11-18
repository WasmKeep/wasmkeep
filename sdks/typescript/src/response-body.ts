// Writing a response body the host already holds; no host import, so
// tests load it under node.
import { describeError } from "./errors";

/** Writes `body` to a response stream that is already handed over, then
 * `finish`. A caller that left meanwhile (its call timed out, its
 * connection closed) closes the stream: that is the caller's outcome, not
 * the actor's failure, so the rest is dropped quietly instead of trapping
 * (a trap would discard the instance and its memory). */
export function writeBody(stream: { blockingWriteAndFlush(b: Uint8Array): void }, body: Uint8Array, finish: () => void): void {
  let left: unknown;
  try {
    for (let at = 0; at < body.length; at += 4096) {
      stream.blockingWriteAndFlush(body.subarray(at, at + 4096));
    }
  } catch (e) {
    left = e;
  }
  (stream as unknown as Disposable)[Symbol.dispose]?.();
  if (left !== undefined) console.error(`response body not delivered (the caller left): ${describeError(left)}`);
  // Finished either way, as the Rust SDK does: the host then ends the
  // response instead of waiting for the rest.
  try {
    finish();
  } catch (e) {
    if (left === undefined) console.error(`response body not finished (the caller left): ${describeError(e)}`);
  }
}
