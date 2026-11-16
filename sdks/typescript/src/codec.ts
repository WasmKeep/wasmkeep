// Runtime codecs for typed operations (./contracts.ts): a value read from
// JSON is checked, not asserted. Pure: nothing here imports the host.
//
// A codec is any object with `parse(input: unknown): T` that throws when
// the input is not a `T`, so a validator you already use (Zod's schemas
// have this shape) works where a codec is expected. The ones below are the
// small set a contract needs, with no dependency.
//
// Objects keep the keys they declare and drop the rest, so a peer that adds
// a field does not break an older reader. `optional` (the key may be
// absent) and `nullable` (the value may be `null`) are different things and
// stay different.

/** Checks `input` and returns it as `T`, or throws. */
export interface Codec<T> {
  parse(input: unknown): T;
}

/** The type a codec produces. */
export type Infer<C> = C extends Codec<infer T> ? T : never;

/** What the built-in codecs throw: where (`$.items[2].sku` style, empty at
 * the top) and why. */
export class CodecError extends Error {
  constructor(
    readonly path: string,
    readonly reason: string,
  ) {
    super(path === "" ? reason : `${path}: ${reason}`);
    this.name = "CodecError";
  }
}

/** A codec for a key an object may leave out. */
export interface OptionalCodec<T> extends Codec<T | undefined> {
  readonly optional: true;
  readonly inner: Codec<T>;
}

function fail(reason: string): never {
  throw new CodecError("", reason);
}

function within<T>(segment: string, f: () => T): T {
  try {
    return f();
  } catch (e) {
    if (e instanceof CodecError) {
      const sep = e.path === "" || e.path.startsWith("[") ? "" : ".";
      throw new CodecError(`${segment}${sep}${e.path}`, e.reason);
    }
    throw e;
  }
}

function describe(v: unknown): string {
  if (v === null) return "null";
  if (Array.isArray(v)) return "an array";
  return typeof v === "object" ? "an object" : typeof v === "undefined" ? "undefined" : `a ${typeof v}`;
}

function own(o: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(o, key);
}

/** A string, at most `maxLength` UTF-16 units when given. */
function string(options: { maxLength?: number } = {}): Codec<string> {
  return {
    parse(v) {
      if (typeof v !== "string") return fail(`expected a string, got ${describe(v)}`);
      if (options.maxLength !== undefined && v.length > options.maxLength) {
        return fail(`longer than ${options.maxLength}`);
      }
      return v;
    },
  };
}

/** A finite number. */
function number(): Codec<number> {
  return {
    parse(v) {
      if (typeof v !== "number" || !Number.isFinite(v)) return fail(`expected a finite number, got ${describe(v)}`);
      return v;
    },
  };
}

/** A whole number a JavaScript number holds exactly. */
function integer(): Codec<number> {
  return {
    parse(v) {
      if (typeof v !== "number" || !Number.isSafeInteger(v)) return fail(`expected a safe integer, got ${describe(v)}`);
      return v;
    },
  };
}

function boolean(): Codec<boolean> {
  return {
    parse(v) {
      if (typeof v !== "boolean") return fail(`expected a boolean, got ${describe(v)}`);
      return v;
    },
  };
}

/** Exactly `null`. */
function nullCodec(): Codec<null> {
  return {
    parse(v) {
      if (v !== null) return fail(`expected null, got ${describe(v)}`);
      return null;
    },
  };
}

/** Anything: the value is not checked. Use it for a payload you pass on. */
function unknown(): Codec<unknown> {
  return { parse: (v) => v };
}

/** No value: an empty body, or no `details`. */
function none(): Codec<void> {
  return {
    parse(v) {
      if (v !== undefined) return fail(`expected no value, got ${describe(v)}`);
    },
  };
}

/** Exactly one of the given strings, numbers or booleans. */
function literal<const V extends string | number | boolean | null>(value: V): Codec<V> {
  return {
    parse(v) {
      if (v !== value) return fail(`expected ${JSON.stringify(value)}`);
      return value;
    },
  };
}

/** One of the given strings. */
function oneOf<const V extends readonly string[]>(values: V): Codec<V[number]> {
  return {
    parse(v) {
      if (typeof v !== "string" || !values.includes(v)) {
        return fail(`expected one of ${values.map((x) => JSON.stringify(x)).join(", ")}`);
      }
      return v;
    },
  };
}

/** An array whose items all pass `item`; at most `maxItems` when given. */
function array<T>(item: Codec<T>, options: { maxItems?: number } = {}): Codec<T[]> {
  return {
    parse(v) {
      if (!Array.isArray(v)) return fail(`expected an array, got ${describe(v)}`);
      if (options.maxItems !== undefined && v.length > options.maxItems) {
        return fail(`more than ${options.maxItems} items`);
      }
      return v.map((x, i) => within(`[${i}]`, () => item.parse(x)));
    },
  };
}

/** An object with any keys, all values passing `value`. */
function record<T>(value: Codec<T>): Codec<Record<string, T>> {
  return {
    parse(v) {
      if (typeof v !== "object" || v === null || Array.isArray(v)) {
        return fail(`expected an object, got ${describe(v)}`);
      }
      const out: Record<string, T> = {};
      for (const k of Object.keys(v)) {
        const parsed = within(k, () => value.parse((v as Record<string, unknown>)[k]));
        // defineProperty: a key named `__proto__` becomes data, not a prototype.
        Object.defineProperty(out, k, { value: parsed, enumerable: true, writable: true, configurable: true });
      }
      return out;
    },
  };
}

/** `inner`, or the key absent (inside {@link object}). `null` is not
 * absent: pair with {@link nullable} to allow both. */
function optional<T>(inner: Codec<T>): OptionalCodec<T> {
  return {
    optional: true,
    inner,
    parse: (v) => (v === undefined ? undefined : inner.parse(v)),
  };
}

/** `inner`, or `null`. */
function nullable<T>(inner: Codec<T>): Codec<T | null> {
  return { parse: (v) => (v === null ? null : inner.parse(v)) };
}

/** The first of `options` that accepts the input. */
function union<const A extends readonly Codec<unknown>[]>(options: A): Codec<Infer<A[number]>> {
  return {
    parse(v) {
      const reasons: string[] = [];
      for (const c of options) {
        try {
          return c.parse(v) as Infer<A[number]>;
        } catch (e) {
          if (!(e instanceof CodecError)) throw e;
          reasons.push(e.message);
        }
      }
      return fail(`no alternative matched (${reasons.join("; ")})`);
    },
  };
}

type Shape = Record<string, Codec<unknown>>;
type OptionalKeys<S extends Shape> = { [K in keyof S]: S[K] extends OptionalCodec<unknown> ? K : never }[keyof S];
type Simplify<T> = { [K in keyof T]: T[K] } & {};

/** The type of an object codec's shape: a key whose codec is `optional`
 * may be left out. */
export type ObjectOf<S extends Shape> = Simplify<
  { [K in Exclude<keyof S, OptionalKeys<S>>]: Infer<S[K]> } & {
    [K in OptionalKeys<S>]?: Exclude<Infer<S[K]>, undefined>;
  }
>;

/** An object with the declared keys. A key absent fails unless its codec
 * is `optional`; keys not declared are dropped. */
function object<const S extends Shape>(shape: S): Codec<ObjectOf<S>> {
  const keys = Object.keys(shape);
  if (keys.includes("__proto__")) throw new TypeError("an object codec cannot declare __proto__");
  return {
    parse(v) {
      if (typeof v !== "object" || v === null || Array.isArray(v)) {
        return fail(`expected an object, got ${describe(v)}`);
      }
      const out: Record<string, unknown> = {};
      for (const k of keys) {
        const c = shape[k];
        if (!own(v, k)) {
          if ((c as Partial<OptionalCodec<unknown>>).optional === true) continue;
          return within(k, () => fail("missing"));
        }
        const raw = (v as Record<string, unknown>)[k];
        const parsed = within(k, () =>
          (c as Partial<OptionalCodec<unknown>>).optional === true ? (c as OptionalCodec<unknown>).inner.parse(raw) : c.parse(raw),
        );
        out[k] = parsed;
      }
      return out as ObjectOf<S>;
    },
  };
}

export {
  string,
  number,
  integer,
  boolean,
  nullCodec as null,
  unknown,
  none,
  literal,
  oneOf,
  array,
  record,
  optional,
  nullable,
  union,
  object,
};
