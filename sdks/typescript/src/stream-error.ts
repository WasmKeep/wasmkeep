// WIT errors as the engines throw them, and resource handles dropped:
// shared by the inbound body (./body.ts) and outbound HTTP (./http.ts).
// Pure: node's tests load it as is.

/** A WIT error's text: jco throws an Error whose `payload` is the error
 * value (its message only says to look there), or the value itself. */
export function describe(e: unknown): string {
  const payload = typeof e === "object" && e !== null && "payload" in e ? (e as { payload: unknown }).payload : e;
  if (typeof payload === "object" && payload !== null && "tag" in payload) {
    const { tag, val } = payload as { tag: string; val?: unknown };
    return val === undefined ? tag : `${tag}: ${JSON.stringify(val, (_k, v) => (typeof v === "bigint" ? `${v}` : v))}`;
  }
  if (e instanceof Error) return e.message;
  return String(e);
}

/** The tag of a `stream-error` thrown by a stream read (`closed` or
 * `last-operation-failed`), however the engine wraps it. */
export function streamErrorTag(e: unknown): string | undefined {
  const payload = typeof e === "object" && e !== null && "payload" in e ? (e as { payload: unknown }).payload : e;
  if (typeof payload === "object" && payload !== null && "tag" in payload) {
    return (payload as { tag: string }).tag;
  }
  return undefined;
}

/** What a `wasi:http` error resource gives: its HTTP error code, if any. */
export type ErrorCodeOf<E> = (error: E) => unknown;

/** A broken stream's cause: the HTTP error code when the host gives one
 * (`codeOf`, `wasi:http`'s `http-error-code`), else its debug text. */
export function describeStreamError<E extends { toDebugString(): string }>(e: unknown, codeOf?: ErrorCodeOf<E>): string {
  const payload = typeof e === "object" && e !== null && "payload" in e ? (e as { payload: unknown }).payload : e;
  const val = typeof payload === "object" && payload !== null ? (payload as { val?: unknown }).val : undefined;
  if (typeof val === "object" && val !== null && "toDebugString" in val) {
    const error = val as E;
    if (codeOf !== undefined) {
      try {
        const code = codeOf(error);
        if (code !== undefined && code !== null) return describe(code);
      } catch {
        // No code: the debug text below.
      }
    }
    return error.toDebugString();
  }
  return describe(e);
}

/** Drops a WIT resource handle now (wasi:http refuses to finish a body
 * whose stream is still open). */
export function dispose(handle: unknown): void {
  if (handle === undefined || handle === null) return;
  const h = handle as Record<symbol, (() => void) | undefined>;
  const key = typeof Symbol.dispose === "symbol" ? Symbol.dispose : Symbol.for("dispose");
  (h[key] ?? h[Symbol.for("dispose")])?.call(handle);
}
