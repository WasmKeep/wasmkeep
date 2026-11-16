// A typed client over the actor transport: sends an operation's request,
// then returns a value that says what happened, never a thrown error for
// anything the callee or the network did. Pure: the transport comes in as
// an argument (./transport.ts binds `actors.call`), so this loads no host.
//
// Every result is `{ ok: true, value }` or `{ ok: false, failure }`, and
// a failure is one of:
//   transport         no answer (a CallError): the callee may or may not have run
//   declared          an error the operation lists, with its details decoded
//   problem           a valid envelope with a code the operation does not list
//   invalid-details   a listed code whose details do not match their codec
//   unsupported-envelope / malformed-envelope   an envelope this SDK cannot read
//   http              a failure without an envelope (a legacy actor, plain text)
//   invalid-response  a success status whose body does not match the output codec
//   invalid-request   the input is not valid: nothing was sent
// Nothing here retries, and nothing decides a call is safe to retry: after
// a `transport` failure or a 5xx a write may have been committed.

import { CallError } from "./errors";
import { CodecError, type Infer } from "./codec";
import type { ErrorMap, InputArgs, Operation, WireRequest, Contract } from "./contracts";
import { classifyFailure, type Problem, type WireAnswer } from "./problem";

/** The most calls one batch holds (the host's limit: a longer batch starts none). */
export const MAX_BATCH = 16;

/** How requests reach actors. `actors.call` and `actors.callMany` are one
 * (see `actorTransport`); a test passes its own. */
export interface Transport {
  /** Throws a {@link CallError} when no answer came. */
  call(address: string, request: WireRequest): WireAnswer;
  /** One entry per call, in input order: the answer or the CallError (returned). */
  callMany(calls: ReadonlyArray<readonly [string, WireRequest]>): Array<WireAnswer | CallError>;
}

type DeclaredOf<E extends ErrorMap> = {
  [C in keyof E & string]: { kind: "declared"; status: number; code: C; message: string; details: Infer<E[C]> };
}[keyof E & string];

export type Failure<E extends ErrorMap = {}> =
  | { kind: "transport"; error: CallError }
  | DeclaredOf<E>
  | { kind: "problem"; status: number; problem: Problem }
  | { kind: "invalid-details"; status: number; problem: Problem; reason: string }
  | { kind: "unsupported-envelope"; status: number; version: number }
  | { kind: "malformed-envelope"; status: number; reason: string }
  | { kind: "http"; status: number; text?: string }
  | { kind: "invalid-response"; status: number; reason: string }
  | { kind: "invalid-request"; reason: string };

export type Result<O, E extends ErrorMap = {}> =
  | { ok: true; status: number; value: O }
  | { ok: false; failure: Failure<E> };

/** One entry of a batch: the address, the operation, then its input. */
export type BatchEntry<I = any, O = any, E extends ErrorMap = any> = readonly [
  address: string,
  operation: Operation<I, O, E>,
  ...input: InputArgs<I>,
];

/** Each entry of `T` checked against its own operation's input type. */
export type CheckedEntries<T extends readonly unknown[]> = {
  [K in keyof T]: T[K] extends readonly [string, Operation<infer I, any, any>, ...unknown[]]
    ? readonly [string, Operation<I, any, any>, ...InputArgs<I>]
    : never;
};

/** The results of a batch, one per entry, in the same order, typed per entry. */
export type BatchResults<T extends readonly unknown[]> = {
  -readonly [K in keyof T]: T[K] extends readonly [string, Operation<any, infer O, infer E>, ...unknown[]]
    ? Result<O, E>
    : never;
};

/** The methods of a contract bound to one actor. */
export type Bound<C extends Contract> = {
  [K in keyof C]: C[K] extends Operation<infer I, infer O, infer E> ? (...input: InputArgs<I>) => Result<O, E> : never;
};

const fail = <E extends ErrorMap>(failure: Failure<E>): { ok: false; failure: Failure<E> } => ({ ok: false, failure });

export class Client {
  constructor(private readonly transport: Transport) {}

  /** Calls `operation` on the actor at `address` (`class/name`) and waits. */
  call<I, O, E extends ErrorMap>(address: string, operation: Operation<I, O, E>, ...input: InputArgs<I>): Result<O, E> {
    let request: WireRequest;
    try {
      request = operation.request(input[0] as I);
    } catch (e) {
      return fail(invalid(e));
    }
    let answer: WireAnswer;
    try {
      answer = this.transport.call(address, request);
    } catch (e) {
      if (e instanceof CallError) return fail({ kind: "transport", error: e });
      throw e;
    }
    return interpret(operation, answer);
  }

  /** Calls several operations at once over `callMany`: every call starts
   * together and the results come back in input order, each its own
   * {@link Result}, so one failure does not hide the others. At most
   * {@link MAX_BATCH} entries; a longer batch sends nothing and every entry is
   * a `transport` failure, as the host does. An entry whose input is not valid
   * is an `invalid-request` and is left out of what is sent.
   *
   * ```ts
   * const [cart, profile] = client.batch([
   *   ["cart/alice", getCart],
   *   ["profile/alice", getProfile],
   * ] as const);
   * ```
   */
  batch<const T extends readonly BatchEntry[]>(entries: T & CheckedEntries<T>): BatchResults<T> {
    const results: Array<Result<unknown, ErrorMap>> = new Array(entries.length);
    if (entries.length > MAX_BATCH) {
      const error = new CallError("failed", `too many calls: ${entries.length} (at most ${MAX_BATCH})`);
      return entries.map(() => fail({ kind: "transport", error })) as BatchResults<T>;
    }
    const sent: Array<readonly [string, WireRequest]> = [];
    const at: number[] = [];
    entries.forEach(([address, operation, ...input], i) => {
      try {
        sent.push([address, operation.request(input[0])]);
        at.push(i);
      } catch (e) {
        results[i] = fail(invalid(e));
      }
    });
    if (sent.length > 0) {
      const answers = this.transport.callMany(sent);
      if (answers.length !== sent.length) throw new Error("callMany returned a different number of results");
      answers.forEach((a, j) => {
        const i = at[j];
        results[i] = a instanceof CallError ? fail({ kind: "transport", error: a }) : interpret(entries[i][1], a);
      });
    }
    return results as BatchResults<T>;
  }

  /** The operations of `contract`, as methods that call the actor at `address`. */
  bind<C extends Contract>(address: string, contract: C): Bound<C> {
    const out: Record<string, unknown> = {};
    for (const key of Object.keys(contract)) {
      out[key] = (...input: unknown[]) => this.call(address, contract[key], ...(input as [unknown]));
    }
    return out as Bound<C>;
  }
}

function invalid(e: unknown): { kind: "invalid-request"; reason: string } {
  if (e instanceof CodecError) return { kind: "invalid-request", reason: e.message };
  throw e;
}

function interpret<O, E extends ErrorMap>(operation: Operation<any, O, E>, answer: WireAnswer): Result<O, E> {
  const { status } = answer;
  if (status >= 200 && status < 300) {
    if (answer.body.length > operation.maxResponseBytes) {
      return fail({ kind: "invalid-response", status, reason: `the body is longer than ${operation.maxResponseBytes} bytes` });
    }
    let value: unknown;
    if (answer.body.length > 0) {
      let text: string;
      try {
        text = new TextDecoder("utf-8", { fatal: true }).decode(answer.body);
      } catch {
        return fail({ kind: "invalid-response", status, reason: "the body is not UTF-8" });
      }
      try {
        value = JSON.parse(text);
      } catch {
        return fail({ kind: "invalid-response", status, reason: "the body is not JSON" });
      }
    }
    try {
      return { ok: true, status, value: operation.output.parse(value) };
    } catch (e) {
      return fail({ kind: "invalid-response", status, reason: e instanceof Error ? e.message : String(e) });
    }
  }
  const f = classifyFailure(answer);
  switch (f.kind) {
    case "plain":
      return fail(f.text === undefined ? { kind: "http", status } : { kind: "http", status, text: f.text });
    case "malformed":
      return fail({ kind: "malformed-envelope", status, reason: f.reason });
    case "unsupported-version":
      return fail({ kind: "unsupported-envelope", status, version: f.version });
    case "problem": {
      const { problem } = f;
      const codec = Object.prototype.hasOwnProperty.call(operation.errors, problem.code)
        ? operation.errors[problem.code]
        : undefined;
      if (codec === undefined) return fail({ kind: "problem", status, problem });
      try {
        const details = codec.parse(problem.details);
        return fail({ kind: "declared", status, code: problem.code, message: problem.message, details } as Failure<E>);
      } catch (e) {
        return fail({ kind: "invalid-details", status, problem, reason: e instanceof Error ? e.message : String(e) });
      }
    }
  }
}
