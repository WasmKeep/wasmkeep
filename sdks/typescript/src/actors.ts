// Calls to the app's actors and the operator's values: what actors and
// fronts share (`wasmkeep:actor/actors`, `wasmkeep:actor/calls` and
// `wasmkeep:actor/env`, all in world `front-core`), re-exported by ./index.ts and ./front.ts.

import * as wkActors from "wasmkeep:actor/actors@0.2.0";
import * as wkCalls from "wasmkeep:actor/calls@0.2.0";
import * as wkEnv from "wasmkeep:actor/env@0.2.0";
import { CallError } from "./errors";

export { CallError } from "./errors";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/** A request to another actor. */
export interface CallRequest {
  method?: string;
  path?: string;
  headers?: Record<string, string>;
  body?: string | Uint8Array;
}

/** Another actor's answer. */
export interface CallResponse {
  status: number;
  headers: Record<string, string>;
  body: Uint8Array;
  text(): string;
}

/** Configuration and secrets the operator gives this class
 * (`wasmkeep serve --env CLASS=FILE`), or the front (`--front-env FILE`). */
export const env = {
  /** The value of `name`, or `undefined` if it is not set. */
  get(name: string): string | undefined {
    // `none` is `null` on QuickJS: keep `undefined` on either engine.
    return wkEnv.get(name) ?? undefined;
  },
};

/** An actor's address. Within an app, `${class}/${name}` is what
 * `actors.call` takes. */
export interface ActorAddress {
  tenant: string;
  class: string;
  name: string;
}

export const actors = {
  /** This actor's own address (requests arrive with `/actor/<class>/<name>`
   * removed, so this is how an actor learns who it is). In a front:
   * `{ tenant, class: "_front", name: "main" }`. */
  self(): ActorAddress {
    const a = wkActors.selfAddress();
    return { tenant: a.tenant, class: a.class, name: a.name };
  },

  /** Calls actor `class/name` (same app) and waits for its response.
   * Other events to this actor wait meanwhile. From a front, the call is
   * inside: it reaches `internal` classes. Throws a {@link CallError}. */
  call(address: string, request: CallRequest = {}): CallResponse {
    let r: wkActors.Response;
    try {
      r = wkActors.call(address, lower(request));
    } catch (e) {
      throw callError(e);
    }
    return lift(r);
  },

  /** Calls several actors at once (ADR 0025): every call starts together,
   * and this returns once all have answered or the batch's deadline
   * passed, one entry per call in input order: its response, or the
   * {@link CallError} it failed with (returned, not thrown). At most 16
   * calls; each is a {@link actors.call} in every other respect.
   *
   * ```ts
   * const [cart, profile] = actors.callMany([
   *   ["cart/alice", { path: "/" }],
   *   ["profile/alice", { path: "/" }],
   * ]);
   * if (cart instanceof CallError) return unavailable(cart);
   * ```
   */
  callMany(calls: ReadonlyArray<readonly [string, CallRequest?]>): Array<CallResponse | CallError> {
    const results = wkCalls.callMany(calls.map(([address, request]) => [address, lower(request ?? {})]));
    return results.map((r) => (r.tag === "ok" ? lift(r.val) : errorOf(r.val)));
  },
};

function lower(request: CallRequest): wkActors.Request {
  const body =
    typeof request.body === "string" ? encoder.encode(request.body) : (request.body ?? new Uint8Array());
  return {
    method: request.method ?? "GET",
    path: request.path ?? "/",
    headers: Object.entries(request.headers ?? {}),
    body,
  };
}

function lift(r: wkActors.Response): CallResponse {
  return {
    status: r.status,
    headers: Object.fromEntries(r.headers),
    body: r.body,
    text: () => decoder.decode(r.body),
  };
}

/** A `call-error` value as a {@link CallError}. */
function errorOf(e: { tag: string; val?: string }): CallError {
  return new CallError(e.tag, e.val ?? e.tag);
}

/** What `actors.call` threw, as a {@link CallError} when it is one. */
function callError(e: unknown): unknown {
  const p = (e as { payload?: { tag: string; val?: string } }).payload;
  return p && typeof p.tag === "string" ? errorOf(p) : e;
}
