// Outbound HTTP from an actor, synchronously.
//
// Actor handlers are synchronous, so `fetch` (absent on QuickJS, a
// promise on StarlingMonkey) cannot be awaited in one. `http.fetch` sends
// the request through `wasi:http/outgoing-handler` and blocks until the
// response, as the Rust SDK's `http::fetch` does; `http.fetchAll` sends
// several at once and waits for them together (`wasi:io/poll`), as
// `Promise.all` over `fetch` would elsewhere. The event waits
// meanwhile (other events to the actor queue); waiting does not count
// against its CPU budget, but does against its wall-clock limit (30 s).
// The host's egress rules apply: public addresses only unless the
// operator allows more, and not the ports browsers block.

import { subscribeDuration } from "wasi:clocks/monotonic-clock@0.2.12";
import { handle } from "wasi:http/outgoing-handler@0.2.12";
import {
  Fields,
  IncomingBody,
  OutgoingBody,
  OutgoingRequest,
  RequestOptions,
  type FutureIncomingResponse,
  type FutureTrailers,
  type IncomingResponse,
  httpErrorCode,
  type Method,
} from "wasi:http/types@0.2.12";
import { poll, type Pollable } from "wasi:io/poll@0.2.12";
import type { InputStream } from "wasi:io/streams@0.2.12";
import { describe, describeStreamError as describeStreamErrorOf, dispose, streamErrorTag } from "./stream-error";
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
  /** Bytes of the response body kept (the rest is dropped unread, and the
   * response is still returned); 16 MiB unless given. */
  limit?: number;
}

/** One request of an {@link fetchAll} batch. */
export interface FetchRequest extends FetchOptions {
  url: string;
}

/** What one request of an {@link fetchAll} batch gave. */
export type FetchResult = { ok: true; response: Fetched } | { ok: false; error: FetchError };

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

/** A broken stream's cause, with its HTTP error code when there is one. */
function describeStreamError(e: unknown): string {
  return describeStreamErrorOf(e, httpErrorCode);
}

/** Sends a request and waits for its response. Throws [`FetchError`] when
 * there is none (the status of a response, 404 or 500 included, is not an
 * error), and when its body breaks before its end (a reset, a close short
 * of the Content-Length or inside a chunk, a timeout between bytes): the
 * message says `incomplete body`, and the bytes that came are not returned
 * as a whole answer. A body longer than `limit` is not an error: its first
 * `limit` bytes are returned, the rest dropped unread. For several
 * requests at once, see {@link fetchAll}. */
export function fetch(url: string, options: FetchOptions = {}): Fetched {
  const [result] = fetchAll([{ ...options, url }]);
  if (!result.ok) throw result.error;
  return result.response;
}

/** Requests a batch keeps open at once: the host's limit per actor
 * (`PER_ACTOR_REQUESTS` in the server's egress rules). */
const MAX_OPEN = 16;
/** Times a request the host refused for having too many open is tried
 * again, `RETRY_WAIT` apart: the host frees a finished request's place a
 * moment after its body ends. */
const RETRIES = 100;
const RETRY_WAIT = 1_000_000n; // 1 ms, in nanoseconds

/** Where one request of a batch is, and the resources it holds. */
interface Item {
  request: FetchRequest;
  phase: "queued" | "head" | "body" | "trailers" | "done";
  /** How many times the host refused it for having too many open. */
  tries: number;
  outgoing?: OutgoingBody;
  future?: FutureIncomingResponse;
  /** Why writing the request body failed, if it did. */
  sending?: unknown;
  response?: IncomingResponse;
  status?: number;
  headers?: Record<string, string>;
  incoming?: IncomingBody;
  stream?: InputStream;
  chunks: Uint8Array[];
  total: number;
  trailers?: FutureTrailers;
  result?: FetchResult;
}

/** Sends every request at once and waits for all of them: one result per
 * request, in the same order, each what {@link fetch} returns or throws.
 * One failing does not fail the others.
 *
 * The requests are open together, their responses and bodies read as they
 * come, so the batch takes about as long as its slowest request rather
 * than the sum. Request bodies are written one after the other, once every
 * request that fits has started. The host lets an actor have 16
 * requests open at once: past 16, the rest start as earlier ones finish.
 * The host's egress rules apply to each. */
export function fetchAll(requests: FetchRequest[]): FetchResult[] {
  const items: Item[] = requests.map((request) => ({ request, phase: "queued", tries: 0, chunks: [], total: 0 }));
  // A refused request is started again only once a retry wait has passed.
  let waited = false;
  for (;;) {
    // Start what fits: every `handle` first, then the bodies.
    let open = items.filter((it) => it.phase === "head" || it.phase === "body" || it.phase === "trailers").length;
    const started: Item[] = [];
    for (const it of items) {
      if (open >= MAX_OPEN) break;
      if (it.phase !== "queued" || (it.tries > 0 && !waited)) continue;
      if (start(it)) {
        open += 1;
        started.push(it);
      }
    }
    for (const it of started) sendBody(it);

    // Advance each as far as it goes without waiting.
    let retrying = false;
    for (const it of items) {
      advance(it);
      if (it.phase === "queued" && it.tries > 0) retrying = true;
    }
    if (items.every((it) => it.phase === "done")) break;
    // Wait for any of them, and for a moment when one is to be retried
    // (then something is always waited for: polling nothing traps).
    const pollables: Pollable[] = [];
    for (const it of items) {
      if (it.phase === "head") pollables.push(it.future!.subscribe());
      else if (it.phase === "body") pollables.push(it.stream!.subscribe());
      else if (it.phase === "trailers") pollables.push(it.trailers!.subscribe());
    }
    const clock = retrying ? pollables.push(subscribeDuration(RETRY_WAIT)) - 1 : -1;
    const ready = poll(pollables);
    waited = clock >= 0 && Array.from(ready).includes(clock);
    // Pollables are their resources' children: dropped before any of
    // those changes.
    for (const p of pollables) dispose(p);
  }
  return items.map((it) => it.result!);
}

/** Ends `it` with `result`, dropping what it holds, each resource before
 * the one it came from. */
function end(it: Item, result: FetchResult): void {
  if (it.stream) dispose(it.stream);
  if (it.incoming) dispose(it.incoming);
  if (it.trailers) dispose(it.trailers);
  if (it.response) dispose(it.response);
  if (it.future) dispose(it.future);
  it.stream = it.incoming = it.trailers = it.response = it.future = undefined;
  it.chunks = [];
  it.phase = "done";
  it.result = result;
}

function failed(it: Item, message: string): void {
  end(it, { ok: false, error: new FetchError(message) });
}

/** Builds the request and hands it to the host; false (and done) if it
 * could not. */
function start(it: Item): boolean {
  const options = it.request;
  const url = options.url;
  try {
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
    try {
      it.future = handle(request, requestOptions);
    } catch (e) {
      dispose(outgoing);
      throw new FetchError(`${url}: ${describe(e)}`);
    }
    it.outgoing = outgoing;
    it.phase = "head";
    return true;
  } catch (e) {
    end(it, { ok: false, error: e instanceof FetchError ? e : new FetchError(`${url}: ${describe(e)}`) });
    return false;
  }
}

/** Writes the request body and finishes it. */
function sendBody(it: Item): void {
  const outgoing = it.outgoing!;
  it.outgoing = undefined;
  const raw = it.request.body;
  const body = typeof raw === "string" ? new TextEncoder().encode(raw) : raw;
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
      // The body's stream closes when the request ends before the guest
      // is done with it: either it failed (refused, no connection), and
      // the response says why where it can, or the server already
      // answered and closed (it had the whole body, or answered early),
      // and that answer is the result.
      dispose(outgoing);
      it.sending = failure;
      return;
    }
  }
  try {
    OutgoingBody.finish(outgoing, undefined);
  } catch (e) {
    failed(it, `${it.request.url}: sending the body: ${describe(e)}`);
  }
}

/** Moves `it` on as far as it goes without waiting. */
function advance(it: Item): void {
  const url = it.request.url;
  const limit = it.request.limit ?? 16 * 1024 * 1024;
  for (;;) {
    if (it.phase === "head") {
      const result = it.future!.get();
      // Not ready yet: `undefined` on StarlingMonkey, `null` on QuickJS (a
      // WIT `none`).
      if (result === undefined || result === null) return;
      if (result.tag !== "ok") {
        failed(
          it,
          it.sending !== undefined
            ? `${url}: sending the body: ${describe(it.sending)}`
            : `${url}: response already taken`,
        );
        continue;
      }
      if (result.val.tag === "err") {
        if (result.val.val.tag === "connection-limit-reached" && it.tries < RETRIES) {
          dispose(it.future);
          it.future = undefined;
          it.sending = undefined;
          it.tries += 1;
          it.phase = "queued";
          return;
        }
        failed(it, `${url}: ${describe(result.val.val)}`);
        continue;
      }
      const response = result.val.val;
      it.response = response;
      it.status = response.status();
      const decoder = new TextDecoder();
      const headers: Record<string, string> = {};
      const fields = response.headers();
      for (const [k, v] of fields.entries()) {
        headers[k] = headers[k] === undefined ? decoder.decode(v) : `${headers[k]}, ${decoder.decode(v)}`;
      }
      dispose(fields);
      it.headers = headers;
      try {
        it.incoming = response.consume();
        it.stream = it.incoming.stream();
      } catch {
        failed(it, `${url}: response body already taken`);
        continue;
      }
      it.phase = "body";
    } else if (it.phase === "body") {
      let ended = false;
      while (it.total < limit) {
        let chunk: Uint8Array;
        try {
          chunk = it.stream!.read(BigInt(Math.min(65_536, limit - it.total)));
        } catch (e) {
          // `closed` is the body's own end; `last-operation-failed` is a
          // broken one (a reset, a close short of the Content-Length or
          // inside a chunk, a timeout between bytes).
          if (streamErrorTag(e) === "closed") {
            ended = true;
            break;
          }
          failed(it, `${url}: incomplete body: ${describeStreamError(e)}`);
          return;
        }
        if (chunk.length === 0) return; // nothing yet
        it.chunks.push(chunk);
        it.total += chunk.length;
      }
      if (!ended) {
        // At the limit, the rest is dropped unread (and not checked).
        end(it, { ok: true, response: fetched(it) });
        return;
      }
      // Closed before the limit: the end, unless the trailers carry an error.
      dispose(it.stream);
      it.stream = undefined;
      const incoming = it.incoming!;
      it.incoming = undefined;
      it.trailers = IncomingBody.finish(incoming);
      it.phase = "trailers";
    } else if (it.phase === "trailers") {
      const ended = it.trailers!.get();
      if (ended === undefined || ended === null) return;
      // Trailers, when there are some, are the future's child: dropped first.
      if (ended.tag === "ok" && ended.val.tag === "ok" && ended.val.val) dispose(ended.val.val);
      if (ended.tag === "err") failed(it, `${url}: incomplete body: its trailers were already taken`);
      else if (ended.val.tag === "err") failed(it, `${url}: incomplete body: ${describe(ended.val.val)}`);
      else end(it, { ok: true, response: fetched(it) });
      return;
    } else {
      return;
    }
  }
}

/** The response `it` read. */
function fetched(it: Item): Fetched {
  const bytes = new Uint8Array(it.total);
  let at = 0;
  for (const c of it.chunks) {
    bytes.set(c, at);
    at += c.length;
  }
  const decoder = new TextDecoder();
  return {
    status: it.status!,
    headers: it.headers!,
    body: bytes,
    text: () => decoder.decode(bytes),
    json: <T>() => JSON.parse(decoder.decode(bytes)) as T,
  };
}
