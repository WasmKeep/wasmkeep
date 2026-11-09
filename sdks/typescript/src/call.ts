// One request to another actor for the `kv` and `queue` clients (the Rust
// SDK's `wire::call`). A failed call becomes a message, worded as in Rust:
// the call failing, not the callee's answer (that is in the status).

import * as wkActors from "wasmkeep:actor/actors@0.2.0";

export interface Reply {
  status: number;
  body: Uint8Array;
}

/** `{ reply }` once the callee answered, or `{ error }` (text) if the call
 * failed. */
export function call(
  address: string,
  method: string,
  path: string,
  headers: [string, string][],
  body: Uint8Array,
): { reply: Reply } | { error: string } {
  let r: wkActors.Response;
  try {
    r = wkActors.call(address, { method, path, headers, body });
  } catch (e) {
    const p = (e as { payload?: { tag?: string; val?: string } }).payload;
    switch (p?.tag) {
      case "invalid-address":
        return { error: `invalid address ${address}: ${p.val ?? ""}` };
      case "reentrant":
        return { error: `call to ${address} would re-enter an actor already in this call chain` };
      case "too-large":
        return { error: `call to ${address}: body too large` };
      case "timeout":
        return { error: `call to ${address} timed out` };
      case "failed":
        return { error: `call to ${address} failed: ${p.val ?? ""}` };
      default:
        throw e;
    }
  }
  return { reply: { status: r.status, body: r.body } };
}
