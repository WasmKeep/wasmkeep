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
import { Body, type BodySource } from "./body";
import type { ActorRequest, ActorResponse } from "./request";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** `request`'s body, as {@link Body} reads it. */
function bodyOf(request: IncomingRequest): BodySource {
  const body = request.consume();
  const stream = body.stream();
  return {
    read(max: number): Uint8Array | undefined {
      try {
        return stream.blockingRead(BigInt(max));
      } catch {
        return undefined; // closed: the body has ended
      }
    },
    // Release the stream, then the body (its parent), whether the body was
    // read to its end or not: a resource left open past the response makes
    // the host fail the request once it ends ("resource has children"),
    // and discard the instance.
    close(): void {
      (stream as unknown as Disposable)[Symbol.dispose]?.();
      const trailers = IncomingBody.finish(body);
      (trailers as unknown as Disposable)[Symbol.dispose]?.();
    },
  };
}

function methodName(m: ReturnType<IncomingRequest["method"]>): string {
  return m.tag === "other" ? m.val : m.tag.toUpperCase();
}

/** `request` as handlers see it. */
export function toRequest(request: IncomingRequest): ActorRequest {
  const body = new Body(() => bodyOf(request));
  function bytes(): Uint8Array;
  function bytes(limit: number): Uint8Array | undefined;
  function bytes(limit?: number): Uint8Array | undefined {
    return body.read(limit);
  }
  function text(): string;
  function text(limit: number): string | undefined;
  function text(limit?: number): string | undefined {
    const b = body.read(limit);
    return b === undefined ? undefined : decoder.decode(b);
  }
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
    bytes,
    text,
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
