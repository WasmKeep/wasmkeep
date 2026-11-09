// Outbound HTTP from an actor, synchronously.
//
// Actor handlers are synchronous, so `fetch` (absent on QuickJS, a
// promise on StarlingMonkey) cannot be awaited in one. `http.fetch` sends
// the request through `wasi:http/outgoing-handler` and blocks until the
// response, as the Rust SDK's `http::fetch` does. The event waits
// meanwhile (other events to the actor queue); waiting does not count
// against its CPU budget, but does against its wall-clock limit (30 s).
// The host's egress rules apply: public addresses only unless the
// operator allows more, and not the ports browsers block.

import { handle } from "wasi:http/outgoing-handler@0.2.12";
import {
  Fields,
  OutgoingBody,
  OutgoingRequest,
  RequestOptions,
  type Method,
} from "wasi:http/types@0.2.12";
import { FetchError, splitUrl } from "./url";

export { FetchError } from "./url";

/** What to send. Every field is optional. */
export interface FetchOptions {
  /** `GET` unless given. */
  method?: string;
  headers?: Record<string, string>;
  body?: string | Uint8Array;
  /** Bounds connecting, the first byte of the response and each gap
   * between bytes; 10 s unless given. */
  timeoutMs?: number;
  /** Bytes of the response body kept (the rest is dropped); 16 MiB unless
   * given. */
  limit?: number;
}

/** A response. */
export interface Fetched {
  status: number;
  headers: Record<string, string>;
  body: Uint8Array;
  /** The body as UTF-8 text. */
  text(): string;
  /** The body parsed as JSON. */
  json<T = unknown>(): T;
}

const METHODS: Record<string, Method["tag"]> = {
  GET: "get",
  HEAD: "head",
  POST: "post",
  PUT: "put",
  DELETE: "delete",
  PATCH: "patch",
  OPTIONS: "options",
};

function describe(e: unknown): string {
  // A WIT error: jco throws an Error whose `payload` is the error value
  // (its message only says to look there), or the value itself.
  const payload = typeof e === "object" && e !== null && "payload" in e ? (e as { payload: unknown }).payload : e;
  if (typeof payload === "object" && payload !== null && "tag" in payload) {
    const { tag, val } = payload as { tag: string; val?: unknown };
    return val === undefined ? tag : `${tag}: ${JSON.stringify(val, (_k, v) => (typeof v === "bigint" ? `${v}` : v))}`;
  }
  if (e instanceof Error) return e.message;
  return String(e);
}

/** Drops a WIT resource handle now (wasi:http refuses to finish a body
 * whose stream is still open). */
function dispose(handle: unknown): void {
  const h = handle as Record<symbol, (() => void) | undefined>;
  const key = typeof Symbol.dispose === "symbol" ? Symbol.dispose : Symbol.for("dispose");
  (h[key] ?? h[Symbol.for("dispose")])?.call(handle);
}

/** Sends a request and waits for its response. Throws [`FetchError`] when
 * there is none (the status of a response, 404 or 500 included, is not an
 * error). A body cut short (a reset, a timeout between bytes) is returned
 * as far as it came, with the status. */
export function fetch(url: string, options: FetchOptions = {}): Fetched {
  const { scheme, authority, path } = splitUrl(url);
  const encoder = new TextEncoder();
  const body = typeof options.body === "string" ? encoder.encode(options.body) : options.body;
  const headers: [string, Uint8Array][] = Object.entries(options.headers ?? {}).map(([k, v]) => [
    k.toLowerCase(),
    encoder.encode(v),
  ]);
  // A known length rather than a chunked body: some servers refuse
  // requests without one.
  if (body && body.length > 0 && !headers.some(([k]) => k === "content-length")) {
    headers.push(["content-length", encoder.encode(String(body.length))]);
  }
  const method = (options.method ?? "GET").toUpperCase();
  let request: OutgoingRequest;
  try {
    request = new OutgoingRequest(Fields.fromList(headers));
  } catch (e) {
    throw new FetchError(`${url}: invalid headers (${describe(e)})`);
  }
  try {
    request.setMethod(METHODS[method] ? ({ tag: METHODS[method] } as Method) : { tag: "other", val: method });
    request.setScheme({ tag: scheme });
    request.setAuthority(authority);
    request.setPathWithQuery(path);
  } catch (e) {
    throw new FetchError(`${url}: invalid request (${describe(e)})`);
  }
  const outgoing = request.body();
  const timeout = BigInt(Math.max(1, Math.round(options.timeoutMs ?? 10_000))) * 1_000_000n;
  const requestOptions = new RequestOptions();
  requestOptions.setConnectTimeout(timeout);
  requestOptions.setFirstByteTimeout(timeout);
  requestOptions.setBetweenBytesTimeout(timeout);

  let future;
  try {
    future = handle(request, requestOptions);
  } catch (e) {
    throw new FetchError(`${url}: ${describe(e)}`);
  }
  // Not ready yet: `undefined` on StarlingMonkey, `null` on QuickJS (a WIT
  // `none`).
  const wait = () => {
    let result = future.get();
    while (result === undefined || result === null) {
      future.subscribe().block();
      result = future.get();
    }
    return result;
  };
  if (body && body.length > 0) {
    const stream = outgoing.write();
    let failure: unknown;
    try {
      for (let at = 0; at < body.length; at += 4096) {
        stream.blockingWriteAndFlush(body.subarray(at, at + 4096));
      }
    } catch (e) {
      failure = e;
    } finally {
      dispose(stream);
    }
    if (failure !== undefined) {
      // The stream closes when the request fails (refused, no
      // connection): the response says why, where it can.
      dispose(outgoing);
      const result = wait();
      if (result.tag === "ok" && result.val.tag === "err") {
        throw new FetchError(`${url}: ${describe(result.val.val)}`);
      }
      throw new FetchError(`${url}: sending the body: ${describe(failure)}`);
    }
  }
  try {
    OutgoingBody.finish(outgoing, undefined);
  } catch (e) {
    throw new FetchError(`${url}: sending the body: ${describe(e)}`);
  }

  const result = wait();
  if (result.tag === "err") throw new FetchError(`${url}: response already taken`);
  if (result.val.tag === "err") throw new FetchError(`${url}: ${describe(result.val.val)}`);
  const response = result.val.val;

  const limit = options.limit ?? 16 * 1024 * 1024;
  const chunks: Uint8Array[] = [];
  let total = 0;
  const incoming = response.consume();
  const stream = incoming.stream();
  while (total < limit) {
    let chunk: Uint8Array;
    try {
      chunk = stream.blockingRead(BigInt(Math.min(65_536, limit - total)));
    } catch {
      break; // closed: the body has ended
    }
    chunks.push(chunk);
    total += chunk.length;
  }
  const bytes = new Uint8Array(total);
  let at = 0;
  for (const c of chunks) {
    bytes.set(c, at);
    at += c.length;
  }
  const decoder = new TextDecoder();
  const responseHeaders: Record<string, string> = {};
  for (const [k, v] of response.headers().entries()) {
    responseHeaders[k] = responseHeaders[k] === undefined ? decoder.decode(v) : `${responseHeaders[k]}, ${decoder.decode(v)}`;
  }
  return {
    status: response.status(),
    headers: responseHeaders,
    body: bytes,
    text: () => decoder.decode(bytes),
    json: <T>() => JSON.parse(decoder.decode(bytes)) as T,
  };
}
