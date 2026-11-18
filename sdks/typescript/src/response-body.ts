// Writing a response body the host already holds; no host import, so
// tests load it under node.
import { describeError } from "./errors";

/** Whether `e` is the stream's `closed`: the reader went away. */
function closed(e: unknown): boolean {
  const p = typeof e === "object" && e !== null ? (e as { payload?: unknown }).payload : undefined;
  return typeof p === "object" && p !== null && (p as { tag?: unknown }).tag === "closed";
}

/** Writes `body` to a response stream that is already handed over, then
 * `finish`. A caller that left meanwhile (its call timed out, its
 * connection closed) closes the stream: that is the caller's outcome, not
 * the actor's failure, so the rest is dropped quietly instead of trapping
 * (a trap would discard the instance and its memory). Any other failure
 * (a body that does not match its content-length, say) is the actor's:
 * it is thrown, after the stream is released. */
export function writeBody(stream: { blockingWriteAndFlush(b: Uint8Array): void }, body: Uint8Array, finish: () => void): void {
  let left = false;
  try {
    for (let at = 0; at < body.length; at += 4096) {
      stream.blockingWriteAndFlush(body.subarray(at, at + 4096));
    }
  } catch (e) {
    (stream as unknown as Disposable)[Symbol.dispose]?.();
    if (!closed(e)) throw e;
    left = true;
    console.error(`response body not delivered (the caller left): ${describeError(e)}`);
  }
  if (!left) (stream as unknown as Disposable)[Symbol.dispose]?.();
  // Finished either way, as the Rust SDK does; after the caller left, a
  // failure to finish is that same departure.
  try {
    finish();
  } catch (e) {
    if (!left) throw e;
  }
}
