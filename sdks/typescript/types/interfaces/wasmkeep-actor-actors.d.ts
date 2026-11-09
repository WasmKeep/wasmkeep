declare module 'wasmkeep:actor/actors@0.2.0' {
  /**
   * Sends `request` to the actor at `address` (`class/name`, within the
   * caller's tenant) and waits for its response. The caller's own event
   * stays in progress meanwhile: other events to the caller wait.
   * 
   * The callee sees `x-wasmkeep-caller: <tenant>/<class>/<name>`, the name
   * percent-encoded (every byte but ASCII letters, digits and `-._~`), so
   * `room 1` arrives as `room%201`. The host removes that header from
   * requests coming from outside, so the callee can trust it.
   */
  export function call(address: string, request: Request): Response;
  /**
   * This actor's own address: an actor does not otherwise know its name
   * or class (requests arrive with `/actor/<class>/<name>` removed, and
   * alarms carry nothing). For links to itself, for telling other actors
   * who it is, and for anything keyed by its name.
   */
  export function selfAddress(): ActorAddress;
  /**
   * A fully buffered request, delivered to the callee's
   * `wasi:http/incoming-handler` like any other request.
   */
  export interface Request {
    method: string,
    /**
     * Path and query, starting with `/`.
     */
    path: string,
    headers: Array<[string, string]>,
    body: Uint8Array,
  }
  export interface Response {
    status: number,
    headers: Array<[string, string]>,
    body: Uint8Array,
  }
  export type CallError = CallErrorInvalidAddress | CallErrorReentrant | CallErrorTooLarge | CallErrorTimeout | CallErrorFailed;
  /**
   * The address is not `class/name`, or the class does not exist.
   */
  export interface CallErrorInvalidAddress {
    tag: 'invalid-address',
    val: string,
  }
  /**
   * The call would re-enter an actor already waiting in this call
   * chain (including the caller itself), which would deadlock.
   */
  export interface CallErrorReentrant {
    tag: 'reentrant',
  }
  /**
   * The request or response exceeded the body limit (16 MB).
   */
  export interface CallErrorTooLarge {
    tag: 'too-large',
  }
  /**
   * The callee did not answer within the event's wall-clock limit.
   */
  export interface CallErrorTimeout {
    tag: 'timeout',
  }
  /**
   * The callee trapped, or the host could not deliver the call.
   */
  export interface CallErrorFailed {
    tag: 'failed',
    val: string,
  }
  /**
   * An actor's address. Within a tenant, `class/name` is what `call`
   * takes; the name is decoded (any text without `/` or control characters).
   */
  export interface ActorAddress {
    tenant: string,
    "class": string,
    name: string,
  }
}
