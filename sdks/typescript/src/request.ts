// Helpers that read a request, for actors and fronts alike (ADR 0023):
// who sent it, its host and its scheme, and the answer to a failed call.
// Pure: nothing here imports the host, so node's tests load it as is.

import { CallError } from "./errors";
import type { ActorAddress } from "./actors";

/** An incoming request. The path includes the query; in an actor it is
 * relative to the actor (`/actor/<class>/<name>` removed). */
export interface ActorRequest {
  method: string;
  path: string;
  /** Header names in lower case; of a repeated header, the last value. */
  headers: Record<string, string>;
  /** Every header in the order received, repeats included. */
  headerList?: [string, string][];
  /** The request's authority: the client's `Host` (see `host`). */
  authority?: string;
  /** The body, read on first use. */
  bytes(): Uint8Array;
  text(): string;
  json<T = unknown>(): T;
}

/** What a handler returns: a body (status 200), or a full response. */
export type ActorResponse =
  | string
  | Uint8Array
  | {
      status?: number;
      headers?: Record<string, string>;
      body?: string | Uint8Array;
    };

/** A JSON response. */
export function json(value: unknown, status = 200): ActorResponse {
  return { status, headers: { "content-type": "application/json" }, body: JSON.stringify(value) };
}

/** Who sent a request, as the host says in `x-wasmkeep-caller`: the app's
 * front, another actor of the app, or a client (`outside`). */
export type Caller = { kind: "front" } | { kind: "actor"; address: ActorAddress } | { kind: "outside" };

/** The first value of header `name` (lower case). */
function first(req: ActorRequest, name: string): string | undefined {
  if (req.headerList) return req.headerList.find(([n]) => n.toLowerCase() === name)?.[1];
  return req.headers[name];
}

function decodeName(s: string): string | undefined {
  if (/%(?![0-9a-fA-F]{2})/.test(s)) return undefined;
  try {
    return decodeURIComponent(s);
  } catch {
    return undefined;
  }
}

/** Reads an `x-wasmkeep-caller` value (`undefined`: the header is absent).
 * Only the host sets it: it removes any a client sends, to actors and to
 * the front, and any a guest sets on `actors.call`. A value is the front
 * only as exactly `<tenant>/_front/main`; a value the host never sends is
 * `outside`. */
export function parseCaller(value: string | undefined): Caller {
  if (value === undefined) return { kind: "outside" };
  const i = value.indexOf("/");
  const j = i < 0 ? -1 : value.indexOf("/", i + 1);
  if (i <= 0 || j < 0) return { kind: "outside" };
  const tenant = value.slice(0, i);
  const cls = value.slice(i + 1, j);
  const name = value.slice(j + 1);
  if (cls === "" || name === "") return { kind: "outside" };
  if (cls === "_front") return name === "main" ? { kind: "front" } : { kind: "outside" };
  if (!/^[a-z]/.test(cls)) return { kind: "outside" };
  const decoded = decodeName(name);
  if (decoded === undefined) return { kind: "outside" };
  return { kind: "actor", address: { tenant, class: cls, name: decoded } };
}

/** Who sent `req`. In an actor:
 * `if (caller(req).kind !== "front") return { status: 403 }`. */
export function caller(req: ActorRequest): Caller {
  return parseCaller(first(req, "x-wasmkeep-caller"));
}

/** The request's authority: the client's `Host`, or the authority of an
 * absolute URI. Behind a proxy, the client's only if the proxy passes it
 * (nginx: `proxy_set_header Host $host`). The client chooses it and the
 * host does not validate it: escape it where it goes (HTML, JSON), and
 * check it against the names you serve before trusting it. */
export function host(req: ActorRequest): string | undefined {
  return req.authority;
}

/** `"https"` when the first `X-Forwarded-Proto` value is `https`, else
 * `"http"`. A client can send that header too: the answer is only as good
 * as the proxy in front of the node, which must replace it. Use it to build
 * links and redirects, never to decide what a request may do. */
export function scheme(req: ActorRequest): "http" | "https" {
  const v = first(req, "x-forwarded-proto");
  return v !== undefined && v.split(",")[0].trim().toLowerCase() === "https" ? "https" : "http";
}

/** The answer for a call that failed: `503` with `Retry-After: 1` for
 * `failed` and `timeout` (busy, overloaded or unreachable, or out of time:
 * retrying may work), `500` for the other kinds and anything that is not a
 * `CallError`. The body is generic: the detail is for the log, not the
 * client. */
export function unavailable(err: unknown): ActorResponse {
  if (err instanceof CallError && (err.kind === "failed" || err.kind === "timeout")) {
    return { status: 503, headers: { "retry-after": "1" }, body: "unavailable, retry\n" };
  }
  return { status: 500, body: "internal error\n" };
}
