// Stands in for `wasmkeep:actor/calls` under node (scripts/test.mjs maps
// the import here): each entry goes to the host ./host-stub.ts installed,
// and what it throws as a `call-error` becomes that entry's `err`, as the
// component model returns `list<result<response, call-error>>`.

import { call, type Request, type Response } from "./host-stub";

export type Result<T, E> = { tag: "ok"; val: T } | { tag: "err"; val: E };
export type CallError = { tag: string; val?: string };

export function callMany(calls: Array<[string, Request]>): Array<Result<Response, CallError>> {
  return calls.map(([address, request]) => {
    try {
      return { tag: "ok", val: call(address, request) };
    } catch (e) {
      const p = (e as { payload?: CallError }).payload;
      if (p && typeof p.tag === "string") return { tag: "err", val: p };
      throw e;
    }
  });
}
