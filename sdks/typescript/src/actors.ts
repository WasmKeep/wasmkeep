// Calls to the app's actors and the operator's values: what actors and
// fronts share (`wasmkeep:actor/actors` and `wasmkeep:actor/env`, both in
// world `front-core`), re-exported by ./index.ts and ./front.ts.

import * as wkActors from "wasmkeep:actor/actors@0.2.0";
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
   * inside: it reaches `internal` classes. */
  call(address: string, request: CallRequest = {}): CallResponse {
    const body =
      typeof request.body === "string" ? encoder.encode(request.body) : (request.body ?? new Uint8Array());
    let r: wkActors.Response;
    try {
      r = wkActors.call(address, {
        method: request.method ?? "GET",
        path: request.path ?? "/",
        headers: Object.entries(request.headers ?? {}),
        body,
      });
    } catch (e) {
      const p = (e as { payload?: { tag: string; val?: string } }).payload;
      if (p && typeof p.tag === "string") throw new CallError(p.tag, p.val ?? p.tag);
      throw e;
    }
    return {
      status: r.status,
      headers: Object.fromEntries(r.headers),
      body: r.body,
      text: () => decoder.decode(r.body),
    };
  },
};
