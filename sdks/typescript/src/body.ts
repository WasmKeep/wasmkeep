// A request body, read once, for actors and fronts alike: whole, or with a
// limit that stops reading past it, so a chunked body (no Content-Length)
// is not buffered whole before its size is known. Pure: the stream is a
// callback (./handler.ts wires it), so node's tests load it as is.

/** Where a body's bytes come from. */
export interface BodySource {
  /** At most `max` bytes (`max >= 1`); `undefined` once the body ended. */
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
