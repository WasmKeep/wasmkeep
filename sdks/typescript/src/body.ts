// A request body, read once, for actors and fronts alike: whole, or with a
// limit that stops reading past it, so a chunked body (no Content-Length)
// is not buffered whole before its size is known. A body that breaks
// before its end (the client went away, a close short of its length, one
// over the host's cap) is an error, never a shorter body. Pure: the WIT
// stream is passed in (./handler.ts wires it), so node's tests load it as
// is.

import { describe, describeStreamError, dispose, streamErrorTag } from "./stream-error";

/** Where a body's bytes come from. */
export interface BodySource {
  /** At most `max` bytes (`max >= 1`); `undefined` once the body ended.
   * Throws when the body broke before its end. */
  read(max: number): Uint8Array | undefined;
  /** Releases the stream, read to its end or not. */
  close(): void;
}

const CHUNK = 65536;

/** A request body: opened on the first read, released once read. */
export class Body {
  #open: (() => BodySource) | undefined;
  #bytes: Uint8Array | undefined;
  /** Set when a read stopped past this limit: the body is longer. */
  #longerThan: number | undefined;
  /** Set when the body broke before its end. */
  #failed: unknown;

  constructor(open: () => BodySource) {
    this.#open = open;
  }

  /** The body; with `limit`, `undefined` when it is longer than `limit`
   * bytes, having read at most `limit + 1` of them (the rest is left
   * unread). After that, a read with a limit no larger is `undefined` too,
   * and any other read throws: the body is gone. */
  read(): Uint8Array;
  read(limit: number | undefined): Uint8Array | undefined;
  read(limit?: number): Uint8Array | undefined {
    if (limit !== undefined && !(Number.isInteger(limit) && limit >= 0)) {
      throw new RangeError(`body limit ${limit}: expected a whole number of bytes`);
    }
    if (this.#bytes !== undefined) {
      return limit !== undefined && this.#bytes.length > limit ? undefined : this.#bytes;
    }
    if (this.#longerThan !== undefined) {
      if (limit !== undefined && limit <= this.#longerThan) return undefined;
      throw new Error(`the request body was too large (over ${this.#longerThan} bytes) and was not read`);
    }
    if (this.#failed !== undefined) throw this.#failed;
    const open = this.#open;
    if (open === undefined) throw new Error("the request body could not be read");
    this.#open = undefined;
    const source = open();
    const chunks: Uint8Array[] = [];
    let total = 0;
    try {
      for (;;) {
        const max = limit === undefined ? CHUNK : Math.min(CHUNK, limit + 1 - total);
        const chunk = source.read(max);
        if (chunk === undefined) break;
        chunks.push(chunk);
        total += chunk.length;
        if (limit !== undefined && total > limit) {
          this.#longerThan = limit;
          return undefined;
        }
      }
    } catch (e) {
      // What came is not the body: kept for no later read.
      this.#failed = e;
      throw e;
    } finally {
      source.close();
    }
    const out = new Uint8Array(total);
    let at = 0;
    for (const c of chunks) {
      out.set(c, at);
      at += c.length;
    }
    this.#bytes = out;
    return out;
  }
}

/** The `wasi:io/streams` input stream of a request body, as far as it is
 * read here. */
export interface InputStreamLike {
  blockingRead(len: bigint): Uint8Array;
}

/** A `wasi:http` `future-trailers`, as far as it is read here. */
export interface FutureTrailersLike {
  subscribe(): unknown;
  /** `none` (`undefined` or `null`) until ready; then the trailers, or why
   * the body failed. */
  get():
    | { tag: "ok"; val: { tag: "ok"; val: unknown } | { tag: "err"; val: unknown } }
    | { tag: "err"; val: unknown }
    | undefined
    | null;
}

/** What {@link incomingSource} needs of a request body. */
export interface IncomingParts<E extends { toDebugString(): string }> {
  /** The body's stream. */
  stream: InputStreamLike;
  /** `incoming-body.finish`: called once the stream is dropped. */
  finish(): FutureTrailersLike;
  /** Blocks until `pollable` is ready (`wasi:io/poll`). */
  wait(pollable: unknown): void;
  /** `http-error-code`, for a broken stream's cause. */
  codeOf?: (error: E) => unknown;
}

/** An incoming request body as a {@link BodySource}: only the stream's
 * `closed` error is its end, and then only if the trailers say it ended
 * well; any other error (`last-operation-failed`: a reset, a close short
 * of the Content-Length, a body over the host's cap) throws. The stream,
 * the trailers and the body are released once, whether read to the end,
 * stopped early or broken. */
export function incomingSource<E extends { toDebugString(): string }>(parts: IncomingParts<E>): BodySource {
  let stream: InputStreamLike | undefined = parts.stream;
  /** Drops the stream, then finishes the body (its parent). */
  const finish = (): FutureTrailersLike | undefined => {
    if (stream === undefined) return undefined;
    dispose(stream);
    stream = undefined;
    return parts.finish();
  };
  return {
    read(max: number): Uint8Array | undefined {
      if (stream === undefined) return undefined;
      let chunk: Uint8Array;
      try {
        chunk = stream.blockingRead(BigInt(max));
      } catch (e) {
        if (streamErrorTag(e) !== "closed") {
          throw new Error(`the request body is incomplete: ${describeStreamError(e, parts.codeOf)}`);
        }
        // Closed: the end, unless the trailers carry an error.
        const trailers = finish()!;
        try {
          let ended = trailers.get();
          if (ended === undefined || ended === null) {
            const ready = trailers.subscribe();
            try {
              parts.wait(ready);
            } finally {
              dispose(ready);
            }
            ended = trailers.get();
          }
          if (ended === undefined || ended === null) {
            throw new Error("the request body is incomplete: its trailers never came");
          }
          if (ended.tag === "err") throw new Error("the request body is incomplete: its trailers were already taken");
          if (ended.val.tag === "err") {
            throw new Error(`the request body is incomplete: ${describe(ended.val.val)}`);
          }
          // Trailers, when there are some, are the future's child: dropped first.
          dispose(ended.val.val);
        } finally {
          dispose(trailers);
        }
        return undefined;
      }
      return chunk;
    },
    close(): void {
      const trailers = finish();
      if (trailers !== undefined) dispose(trailers);
    },
  };
}
