// The `wasi:http` handler plumbing shared by actors (./index.ts) and fronts
// (./front.ts): the incoming request as an `ActorRequest`, and a handler's
// `ActorResponse` written out.

import {
  Fields,
  IncomingBody,
  OutgoingBody,
  OutgoingResponse,
  ResponseOutparam,
  type IncomingRequest,
} from "wasi:http/types@0.2.12";
import type { ActorRequest, ActorResponse } from "./request";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

function readAll(request: IncomingRequest): Uint8Array {
  const body = request.consume();
  const stream = body.stream();
  const chunks: Uint8Array[] = [];
  let total = 0;
  for (;;) {
    let chunk: Uint8Array;
    try {
      chunk = stream.blockingRead(65536n);
    } catch {
      break; // closed: the body has ended
    }
    chunks.push(chunk);
    total += chunk.length;
  }
  // Release the stream, then the body (its parent): a resource left open
  // past the response makes the host fail the request once it ends
  // ("resource has children"), and discard the instance.
  (stream as unknown as Disposable)[Symbol.dispose]?.();
  const trailers = IncomingBody.finish(body);
  (trailers as unknown as Disposable)[Symbol.dispose]?.();
  const out = new Uint8Array(total);
  let at = 0;
  for (const c of chunks) {
    out.set(c, at);
    at += c.length;
  }
  return out;
}

function methodName(m: ReturnType<IncomingRequest["method"]>): string {
  return m.tag === "other" ? m.val : m.tag.toUpperCase();
}

/** `request` as handlers see it. */
export function toRequest(request: IncomingRequest): ActorRequest {
  let body: Uint8Array | undefined;
  const headerList = request
    .headers()
    .entries()
    .map(([k, v]) => [k, decoder.decode(v)] as [string, string]);
  const req: ActorRequest = {
    method: methodName(request.method()),
    path: request.pathWithQuery() ?? "/",
    headers: Object.fromEntries(headerList),
    headerList,
    // `none` is `null` on QuickJS.
    authority: request.authority() ?? undefined,
    bytes: () => (body ??= readAll(request)),
    text: () => decoder.decode(req.bytes()),
    json: <T>() => JSON.parse(req.text()) as T,
  };
  return req;
}

/** Writes `response` to `out`. */
export function respond(out: ResponseOutparam, response: ActorResponse): void {
  // Bytes read from SQL on StarlingMonkey are Uint8Arrays of another realm
  // (not `instanceof Uint8Array` here): any typed-array view is a body.
  const r = typeof response === "string" || ArrayBuffer.isView(response) ? { body: response as string | Uint8Array } : response;
  const body = typeof r.body === "string" ? encoder.encode(r.body) : (r.body ?? new Uint8Array());
  const headers: Record<string, string> = { "content-type": "text/plain; charset=utf-8", ...r.headers };
  const fields = Fields.fromList(
    Object.entries(headers).map(([k, v]) => [k.toLowerCase(), encoder.encode(v)] as [string, Uint8Array]),
  );
  const outgoing = new OutgoingResponse(fields);
  outgoing.setStatusCode(r.status ?? 200);
  const outBody = outgoing.body();
  ResponseOutparam.set(out, { tag: "ok", val: outgoing });
  const stream = outBody.write();
  for (let at = 0; at < body.length; at += 4096) {
    stream.blockingWriteAndFlush(body.subarray(at, at + 4096));
  }
  (stream as unknown as Disposable)[Symbol.dispose]?.();
  OutgoingBody.finish(outBody, undefined);
}
