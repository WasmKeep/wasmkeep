// The error an actor call throws; its own module so that the request
// helpers (./request.ts) and the typed clients (./client.ts) use it without
// importing the host.

/** The kinds the host reports for a call that got no answer (the WIT
 * `call-error` cases). */
export const CALL_ERROR_KINDS = ["invalid-address", "reentrant", "too-large", "timeout", "failed"] as const;

/** One of {@link CALL_ERROR_KINDS}. */
export type CallErrorKind = (typeof CALL_ERROR_KINDS)[number];

/** Whether `kind` is a kind this SDK knows. A newer host may report one
 * it does not: such an error keeps its `kind` string and has no `known`. */
export function isCallErrorKind(kind: string): kind is CallErrorKind {
  return (CALL_ERROR_KINDS as readonly string[]).includes(kind);
}

/** A call that did not get an answer: a transport failure, never the
 * callee's own HTTP answer. `kind`: invalid-address, reentrant, too-large,
 * timeout, failed (`string`, so that a kind from a newer host passes
 * through; `known` narrows it). A timeout or `failed` does not say the
 * callee did nothing: a write may have been committed. */
export class CallError extends Error {
  constructor(
    readonly kind: string,
    message: string,
  ) {
    super(message);
    this.name = "CallError";
  }

  /** `kind` when it is one of {@link CALL_ERROR_KINDS}, else `undefined`. */
  get known(): CallErrorKind | undefined {
    return isCallErrorKind(this.kind) ? this.kind : undefined;
  }
}
