// The structured error envelope: an opt-in way for an actor to answer an
// HTTP failure the caller can act on. Pure: nothing here imports the host,
// and the Rust SDK (`wasmkeep_sdk::problem`) writes and reads the same bytes.
//
// It is only an HTTP response convention. It is not a transport failure (a
// `CallError`: no answer came) and not a storage failure (a `SqlError`, or
// a write that is not known to be durable). An answer is an envelope only
// when its status is 400 or more and its content type is
// `application/vnd.wasmkeep.error+json`; any other failure is "plain" (a
// legacy or foreign answer) and stays as it is.
//
// Body (UTF-8 JSON, at most 16 KiB):
//   {"v":1,"code":"cart.out_of_stock","message":"...","details":{...}}
// `code` is stable and meant for programs; `message` is public text meant
// for people; `details` is optional JSON. Fields a reader does not know are
// ignored, a version it does not know is reported, not guessed at. Nothing
// here says whether to retry: a timeout or a 503 does not prove a write was
// not committed, so a retry is the application's decision.

import type { ActorResponse } from "./request";

export const PROBLEM_CONTENT_TYPE = "application/vnd.wasmkeep.error+json";
export const PROBLEM_VERSION = 1;
/** The longest envelope body, in bytes. */
export const MAX_PROBLEM_BYTES = 16 * 1024;
export const MAX_CODE_LENGTH = 64;
export const MAX_MESSAGE_LENGTH = 512;
/** How deep `details` may nest, and the most members one of its objects
 * may have (the Rust reader enforces the same limits). */
export const MAX_DETAILS_DEPTH = 8;
export const MAX_DETAILS_MEMBERS = 64;

const CODE = /^[A-Za-z][A-Za-z0-9_.:-]*$/;

export type JsonValue = null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };

/** A public, structured error. */
export interface Problem {
  code: string;
  message: string;
  details?: JsonValue;
}

/** What an answer with a status that is not a success turns out to be. */
export type FailureAnswer =
  | { kind: "problem"; status: number; problem: Problem }
  /** An envelope of a version this SDK does not read. */
  | { kind: "unsupported-version"; status: number; version: number }
  /** Claims to be an envelope but is not a valid one. */
  | { kind: "malformed"; status: number; reason: string }
  /** No envelope: a legacy, foreign or plain-text failure. `text` is the
   * start of the body when it is UTF-8 (at most 512 characters). */
  | { kind: "plain"; status: number; text?: string };

export interface WireAnswer {
  status: number;
  headers: Record<string, string>;
  body: Uint8Array;
}

/** Whether `code` is a valid error code. */
export function isProblemCode(code: string): boolean {
  return code.length <= MAX_CODE_LENGTH && CODE.test(code);
}

/** Why `value` cannot be `details`, or `undefined` if it can. */
export function detailsFault(value: unknown, depth = 1): string | undefined {
  if (value === null || typeof value === "boolean") return undefined;
  if (typeof value === "string") return LONE.test(value) ? "a string that is not valid Unicode" : undefined;
  if (typeof value === "number") return Number.isFinite(value) ? undefined : "a number that is not finite";
  if (depth > MAX_DETAILS_DEPTH) return `nested deeper than ${MAX_DETAILS_DEPTH}`;
  if (Array.isArray(value)) {
    for (const v of value) {
      const f = detailsFault(v, depth + 1);
      if (f) return f;
    }
    return undefined;
  }
  if (typeof value === "object") {
    const keys = Object.keys(value);
    if (keys.length > MAX_DETAILS_MEMBERS) return `an object of more than ${MAX_DETAILS_MEMBERS} members`;
    for (const k of keys) {
      if (LONE.test(k)) return "a key that is not valid Unicode";
      const f = detailsFault((value as Record<string, unknown>)[k], depth + 1);
      if (f) return f;
    }
    return undefined;
  }
  return `a ${typeof value}`;
}

/** The response for `problem` with `status` (400 to 599). Throws a
 * `RangeError` for a code, message, details or size outside the limits:
 * that is a mistake in the handler, found when it is written. Both fields
 * are public: leave out stack traces, queries, storage locations and
 * credentials. */
export function problemResponse(status: number, problem: Problem): Exclude<ActorResponse, string | Uint8Array> {
  if (!Number.isInteger(status) || status < 400 || status > 599) throw new RangeError("status must be 400 to 599");
  if (typeof problem.message === "string" && LONE.test(problem.message)) throw new RangeError("message is not valid Unicode");
  if (!isProblemCode(problem.code)) throw new RangeError(`invalid error code ${JSON.stringify(problem.code)}`);
  if (typeof problem.message !== "string" || problem.message.length > MAX_MESSAGE_LENGTH) {
    throw new RangeError(`message must be a string of at most ${MAX_MESSAGE_LENGTH} characters`);
  }
  const wire: Record<string, unknown> = { v: PROBLEM_VERSION, code: problem.code, message: problem.message };
  if (problem.details !== undefined) {
    const f = detailsFault(problem.details);
    if (f) throw new RangeError(`details: ${f}`);
    wire.details = problem.details;
  }
  const body = JSON.stringify(wire);
  if (new TextEncoder().encode(body).length > MAX_PROBLEM_BYTES) throw new RangeError("the error is too large");
  return { status, headers: { "content-type": PROBLEM_CONTENT_TYPE }, body };
}

/** Shorthand for {@link problemResponse}. */
export function problem(status: number, code: string, message: string, details?: JsonValue) {
  return problemResponse(status, details === undefined ? { code, message } : { code, message, details });
}

function header(headers: Record<string, string>, name: string): string | undefined {
  for (const k of Object.keys(headers)) if (k.toLowerCase() === name) return headers[k];
  return undefined;
}

/** Classifies an answer whose status is not a success. */
export function classifyFailure(answer: WireAnswer): FailureAnswer {
  const { status } = answer;
  const media = header(answer.headers, "content-type")?.split(";")[0].trim().toLowerCase();
  if (media !== PROBLEM_CONTENT_TYPE || status < 400) {
    return { kind: "plain", status, text: preview(answer.body) };
  }
  const malformed = (reason: string): FailureAnswer => ({ kind: "malformed", status, reason });
  if (answer.body.length > MAX_PROBLEM_BYTES) return malformed("the body is too large");
  let text: string;
  try {
    text = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(answer.body);
  } catch {
    return malformed("the body is not UTF-8");
  }
  // The parse-time rules both SDKs share, checked on the text before it is
  // parsed or walked, so hostile input cannot reach the recursion.
  const fault = textFault(text);
  if (fault) return malformed(fault);
  let json: unknown;
  try {
    json = JSON.parse(text);
  } catch {
    return malformed("the body is not JSON");
  }
  if (typeof json !== "object" || json === null || Array.isArray(json)) return malformed("the body is not an object");
  const o = json as Record<string, unknown>;
  const v = o.v;
  if (typeof v !== "number" || !Number.isSafeInteger(v)) return malformed("v is missing or not an integer");
  if (v !== PROBLEM_VERSION) return { kind: "unsupported-version", status, version: v };
  if (typeof o.code !== "string" || !isProblemCode(o.code)) return malformed("code is missing or invalid");
  if (typeof o.message !== "string" || o.message.length > MAX_MESSAGE_LENGTH) {
    return malformed("message is missing or too long");
  }
  const p: Problem = { code: o.code, message: o.message };
  if (Object.prototype.hasOwnProperty.call(o, "details")) {
    const f = detailsFault(o.details);
    if (f) return malformed(`details: ${f}`);
    p.details = o.details as JsonValue;
  }
  return { kind: "problem", status, problem: p };
}

const LONE = /[\ud800-\udbff](?![\udc00-\udfff])|(?<![\ud800-\udbff])[\udc00-\udfff]/;

/** The deepest nesting of arrays and objects in a body, counting the
 * envelope's own object (the Rust reader's JSON limit). */
export const MAX_JSON_DEPTH = 16;

/** Why `text` breaks a rule the Rust reader applies while parsing, or
 * `undefined`: nesting deeper than {@link MAX_JSON_DEPTH}, an object with
 * more than {@link MAX_DETAILS_MEMBERS} members, a repeated key (the last
 * would win and could hide a bad value), or a lone surrogate in a string or
 * key. A scan of the text with an explicit stack: no recursion, one pass.
 * It does not check the syntax; `JSON.parse` does, afterwards. */
function textFault(text: string): string | undefined {
  const stack: Array<{ object: boolean; keys: Set<string>; expectKey: boolean }> = [];
  const n = text.length;
  for (let i = 0; i < n; i++) {
    const c = text.charCodeAt(i);
    if (c === 0x7b /* { */ || c === 0x5b /* [ */) {
      if (stack.length >= MAX_JSON_DEPTH) return "nested too deeply";
      stack.push({ object: c === 0x7b, keys: new Set(), expectKey: c === 0x7b });
    } else if (c === 0x7d || c === 0x5d) {
      stack.pop();
    } else if (c === 0x2c /* , */) {
      const top = stack[stack.length - 1];
      if (top?.object) top.expectKey = true;
    } else if (c === 0x3a /* : */) {
      const top = stack[stack.length - 1];
      if (top) top.expectKey = false;
    } else if (c === 0x22 /* " */) {
      let j = i + 1;
      while (j < n && text.charCodeAt(j) !== 0x22) j += text.charCodeAt(j) === 0x5c ? 2 : 1;
      if (j >= n) return undefined; // unterminated: JSON.parse reports it
      let value: string;
      try {
        value = JSON.parse(text.slice(i, j + 1)) as string;
      } catch {
        return undefined; // a bad escape: JSON.parse reports it
      }
      if (LONE.test(value)) return "a string is not valid Unicode";
      const top = stack[stack.length - 1];
      if (top?.object && top.expectKey) {
        if (top.keys.has(value)) return "a key is repeated";
        top.keys.add(value);
        if (top.keys.size > MAX_DETAILS_MEMBERS) return "too many members";
        top.expectKey = false;
      }
      i = j;
    }
  }
  return undefined;
}

function preview(body: Uint8Array): string | undefined {
  if (body.length > 64 * 1024) return undefined;
  try {
    const t = new TextDecoder("utf-8", { fatal: true }).decode(body);
    return t.length > 512 ? t.slice(0, 512) : t;
  } catch {
    return undefined;
  }
}
