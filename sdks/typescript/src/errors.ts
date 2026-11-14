// The error an actor call throws; its own module so that the request
// helpers (./request.ts) use it without importing the host.

/** A call that did not get an answer. `kind`: invalid-address, reentrant,
 * too-large, timeout, failed. */
export class CallError extends Error {
  constructor(
    readonly kind: string,
    message: string,
  ) {
    super(message);
    this.name = "CallError";
  }
}
