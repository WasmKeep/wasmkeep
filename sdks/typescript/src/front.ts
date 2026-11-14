/**
 * Writing a front (ADR 0023): the app's stateless `wasi:http` component,
 * which receives every request outside `/actor/` and `/_wasmkeep/`, holds
 * no state, and calls the app's actors as an inside caller.
 *
 * ```ts
 * import { actors, defineFront, unavailable } from "@wasmkeep/sdk/front";
 *
 * export default defineFront({
 *   fetch(req) {
 *     try {
 *       return actors.call("todos/alice", { path: "/" }).text();
 *     } catch (e) {
 *       return unavailable(e);
 *     }
 *   },
 * });
 * ```
 *
 * Build with `wasmkeep-ts build src/front.ts --world front -o front.wasm`.
 * Import from `@wasmkeep/sdk/front` only: `@wasmkeep/sdk` brings the actor
 * world (storage, alarms, WebSockets), which a front does not have.
 */

/// <reference path="./globals.d.ts" />
/// <reference path="../types/actor.d.ts" />
// First: installs TextEncoder/TextDecoder on engines without them (QuickJS).
import "./text";
import "./console";
import type { ResponseOutparam, IncomingRequest } from "wasi:http/types@0.2.12";
import { explainMissing } from "./engines";
import { respond, toRequest } from "./handler";
import type { ActorRequest, ActorResponse } from "./request";

export { actors, env, CallError, type ActorAddress, type CallRequest, type CallResponse } from "./actors";
export {
  caller,
  host,
  json,
  parseCaller,
  scheme,
  unavailable,
  type ActorRequest,
  type ActorResponse,
  type Caller,
} from "./request";
/** Synchronous outbound HTTP (`http.fetch(url, {...})`); see ./http.ts. */
export * as http from "./http";

/** A front: its request handler. */
export interface FrontDefinition {
  /** Handles an HTTP request. */
  fetch(req: ActorRequest): ActorResponse;
}

/** Declares a front; `export default` it from the entry module. */
export function defineFront(definition: FrontDefinition): FrontDefinition {
  return definition;
}

/** The component's export for `definition`. Called by the build's
 * generated entry module (`--world front`); not needed in front code. */
export function frontExportsFor(definition: FrontDefinition) {
  return {
    incomingHandler: {
      handle(request: IncomingRequest, out: ResponseOutparam): void {
        let response: ActorResponse;
        try {
          response = definition.fetch(toRequest(request));
        } catch (e) {
          console.error(explainMissing(e instanceof Error ? e.message : String(e)));
          response = { status: 500, body: "internal error\n" };
        }
        respond(out, response);
      },
    },
  };
}
