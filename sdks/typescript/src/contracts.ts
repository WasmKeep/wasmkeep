// Typed operation contracts: what one call to an actor looks like (its
// name, method, path, input, answer and declared errors) as a value both
// sides can import. Pure: no host import, so a front, an actor, a test and
// an ordinary TypeScript program share a contract file.
//
//   const addItem = defineOperation({
//     name: "cart.addItem",
//     method: "POST",
//     path: (i) => urlPath`/items/${i.sku}`,
//     input: codec.object({ sku: codec.string(), qty: codec.integer() }),
//     output: codec.object({ count: codec.integer() }),
//     errors: { "cart.out_of_stock": codec.object({ left: codec.integer() }) },
//   });
//
// The input is checked before anything is sent and the answer is checked
// before it is returned (./client.ts): the types come from the codecs, so
// they cannot drift from what is validated.

import { CodecError, type Codec } from "./codec";

/** A request to send, as the transport takes it. */
export interface WireRequest {
  method: string;
  path: string;
  headers: Record<string, string>;
  body?: Uint8Array;
}

/** Declared application errors: code to the codec of its `details`
 * (`codec.none()` for an error that has none). */
export type ErrorMap = { readonly [code: string]: Codec<unknown> };

/** The arguments an operation's client method takes: none for a `void`
 * input. */
export type InputArgs<I> = [I] extends [void] ? [input?: I] : [input: I];

export interface Operation<I = void, O = void, E extends ErrorMap = {}> {
  readonly name: string;
  readonly method: string;
  readonly input: Codec<I>;
  readonly output: Codec<O>;
  readonly errors: E;
  /** The longest success body the client decodes, in bytes. */
  readonly maxResponseBytes: number;
  /** The request for `input`. Throws a {@link CodecError} when `input`
   * is not valid or the path cannot be built safely. */
  request(input: I): WireRequest;
}

/** Operations by method name: what a client binds to an actor. */
export type Contract = { readonly [method: string]: Operation<any, any, any> };

export interface OperationSpec<I, O, E extends ErrorMap> {
  /** A stable name for logs and errors; not sent. */
  name: string;
  /** Default `GET`. */
  method?: string;
  /** The path (start with `/`, query allowed), built with {@link urlPath}
   * and {@link withQuery} so that values are encoded. */
  path: string | ((input: I) => string);
  /** Default: no input (`void`). */
  input?: Codec<I>;
  /** Default: no body expected (`void`). */
  output?: Codec<O>;
  errors?: E;
  /** The JSON body to send. Default: the input, as JSON, except for `GET`,
   * `HEAD` and `DELETE`, which send none (the input then goes into the path). */
  body?: (input: I) => unknown;
  /** Default 1 MiB. */
  maxResponseBytes?: number;
}

const NO_BODY = new Set(["GET", "HEAD", "DELETE"]);
const noInput: Codec<void> = {
  parse(v) {
    if (v !== undefined) throw new CodecError("", "this operation takes no input");
  },
};
const noOutput: Codec<void> = {
  parse(v) {
    if (v !== undefined) throw new CodecError("", "expected an empty answer");
  },
};

/** Declares an operation. The types of `I`, `O` and `E` come from the codecs. */
export function defineOperation<I = void, O = void, E extends ErrorMap = {}>(
  spec: OperationSpec<I, O, E>,
): Operation<I, O, E> {
  if (spec.name === "") throw new TypeError("an operation needs a name");
  const method = (spec.method ?? "GET").toUpperCase();
  if (!/^[A-Z]+$/.test(method)) throw new TypeError(`invalid method ${spec.method}`);
  for (const code of Object.keys(spec.errors ?? {})) {
    if (!/^[A-Za-z][A-Za-z0-9_.:-]{0,63}$/.test(code)) throw new TypeError(`invalid error code ${code}`);
  }
  const input = spec.input ?? (noInput as Codec<unknown> as Codec<I>);
  const output = spec.output ?? (noOutput as Codec<unknown> as Codec<O>);
  const encoder = new TextEncoder();
  return {
    name: spec.name,
    method,
    input,
    output,
    errors: (spec.errors ?? {}) as E,
    maxResponseBytes: spec.maxResponseBytes ?? 1024 * 1024,
    request(raw: I): WireRequest {
      // Whatever a codec, the path function or the body function throws (a
      // third-party validator throws its own errors) is the input being
      // invalid: the caller gets a CodecError, nothing is sent.
      try {
        return build(raw);
      } catch (e) {
        if (e instanceof CodecError) throw e;
        throw new CodecError("", e instanceof Error ? e.message : String(e));
      }
    },
  };

  function build(raw: I): WireRequest {
    const value = input.parse(raw);
    const path = typeof spec.path === "function" ? spec.path(value) : spec.path;
    checkPath(path);
    const headers: Record<string, string> = { accept: "application/json" };
    const payload = spec.body ? spec.body(value) : NO_BODY.has(method) ? undefined : value;
    if (payload === undefined) return { method, path, headers };
    const text = JSON.stringify(payload);
    if (text === undefined) return { method, path, headers };
    headers["content-type"] = "application/json";
    return { method, path, headers, body: encoder.encode(text) };
  }
}

/** Groups operations for a client (`client.bind(address, contract)`). */
export function defineContract<const C extends Contract>(operations: C): C {
  return operations;
}

function checkPath(path: string): void {
  if (!path.startsWith("/")) throw new CodecError("", "path must start with /");
  // eslint-disable-next-line no-control-regex
  if (/[\u0000- \u007f#\\]/.test(path)) throw new CodecError("", "path has a space, control character, # or backslash");
  // A dot segment, written or percent-encoded, is resolved away by URL
  // normalization: it would name another path than the one built.
  for (const seg of path.split("?")[0].split("/")) {
    if (/^(?:\.|%2e)(?:\.|%2e)?$/i.test(seg)) throw new CodecError("", "path has a . or .. segment");
  }
}

function scalar(value: unknown): string {
  if (typeof value !== "string" && typeof value !== "number" && typeof value !== "boolean" && typeof value !== "bigint") {
    throw new CodecError("", `a path value must be a string, number, boolean or bigint, not ${typeof value}`);
  }
  const s = String(value);
  try {
    return encodeURIComponent(s).replace(/[!'()*]/g, (c) => "%" + c.charCodeAt(0).toString(16).toUpperCase());
  } catch {
    throw new CodecError("", "a path value is not valid text (a lone surrogate)");
  }
}

function segment(value: unknown): string {
  const s = scalar(value);
  if (s === "." || s === "..") throw new CodecError("", "a path value cannot be . or ..");
  return s;
}

/** A tagged template that percent-encodes every interpolated value as one
 * path segment (`/` in a value becomes `%2F`; a value of `.` or `..` is refused):
 * ``urlPath`/carts/${id}/items` ``. */
export function urlPath(strings: TemplateStringsArray, ...values: unknown[]): string {
  let out = strings[0];
  values.forEach((v, i) => {
    out += segment(v) + strings[i + 1];
  });
  return out;
}

/** Appends a query string to `path`. A key with an `undefined` value is
 * left out; an array repeats the key; `null` is not a query value. */
export function withQuery(
  path: string,
  query: Record<string, string | number | boolean | undefined | ReadonlyArray<string | number | boolean>>,
): string {
  const parts: string[] = [];
  for (const k of Object.keys(query)) {
    const v = query[k];
    if (v === undefined) continue;
    for (const item of Array.isArray(v) ? v : [v]) {
      if (typeof item !== "string" && typeof item !== "number" && typeof item !== "boolean") {
        throw new CodecError("", `query value of ${k} is not a string, number or boolean`);
      }
      parts.push(`${scalar(k)}=${scalar(item)}`);
    }
  }
  return parts.length === 0 ? path : `${path}${path.includes("?") ? "&" : "?"}${parts.join("&")}`;
}
