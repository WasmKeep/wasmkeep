/// <reference path="./wasmkeep-actor-actors.d.ts" />
declare module 'wasmkeep:actor/calls@0.2.0' {
  /**
   * Sends each request to its actor (`class/name`, within the caller's
   * tenant), all at once, and waits until every one has answered or the
   * batch's deadline passed. Returns one result per entry, in input
   * order. Each entry is an `actors.call`: the same caller value, chain,
   * routing and errors, so an entry naming the caller or an actor waiting
   * in its chain is `reentrant`. The caller's event stays in progress
   * meanwhile, and each callee still runs its events one at a time: no
   * order between entries is promised, even to one callee. Entries to the
   * same actor are allowed; each is its own call.
   * 
   * Limits, for the batch as a whole:
   * - at most 16 entries: a longer batch starts no call, and every entry
   *   is `failed("too many calls: ...")`;
   * - one deadline, fixed when the call starts (an actor's call limit,
   *   or a front's call deadline): an entry still waiting then is
   *   `timeout`, and the others keep their answers;
   * - request bodies together at most 16 MB (else every entry is
   *   `too-large` and none starts), responses together at most 16 MB
   *   (an entry whose answer would pass it is `too-large`).
   * 
   * Entries of one batch are not in each other's chain: if two callees
   * of a batch call each other, both wait until their deadline and end
   * in `timeout`.
   */
  export function callMany(calls: Array<[string, Request]>): Array<Result<Response, CallError>>;
  export type Request = import('wasmkeep:actor/actors@0.2.0').Request;
  export type Response = import('wasmkeep:actor/actors@0.2.0').Response;
  export type CallError = import('wasmkeep:actor/actors@0.2.0').CallError;
  export type Result<T, E> = { tag: 'ok', val: T } | { tag: 'err', val: E };
}
