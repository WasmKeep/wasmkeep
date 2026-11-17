// Headers as the wire carries them: an ordered list of (name, value) pairs
// in which a name may repeat. Names compare without case. Pure: nothing here
// imports the host, so node's tests load it as is.

/** What a request or response may be given as headers: a record (a value may
 * be a list, one pair each), pairs in order, or a {@link HeaderList}. */
export type HeadersInit =
  | Record<string, string | readonly string[]>
  | ReadonlyArray<readonly [string, string]>
  | HeaderList;

/** An ordered list of header pairs; a name may repeat, and compares without
 * case. `get` is the first value, `getAll` every value in order, `append`
 * adds, `set` replaces every value. A `Set-Cookie` is never joined. */
export class HeaderList implements Iterable<[string, string]> {
  private readonly list: [string, string][];

  constructor(init?: HeadersInit) {
    this.list = pairsOf(init);
  }

  /** The first value of `name`. */
  get(name: string): string | undefined {
    const lower = name.toLowerCase();
    return this.list.find(([n]) => n.toLowerCase() === lower)?.[1];
  }

  /** Every value of `name`, in order. */
  getAll(name: string): string[] {
    const lower = name.toLowerCase();
    return this.list.filter(([n]) => n.toLowerCase() === lower).map(([, v]) => v);
  }

  has(name: string): boolean {
    return this.get(name) !== undefined;
  }

  /** Adds a value after the others. */
  append(name: string, value: string): this {
    this.list.push([name, value]);
    return this;
  }

  /** Replaces every value of `name` with `value`, at the place of its first. */
  set(name: string, value: string): this {
    const lower = name.toLowerCase();
    const at = this.list.findIndex(([n]) => n.toLowerCase() === lower);
    if (at < 0) return this.append(name, value);
    this.list[at] = [name, value];
    for (let i = this.list.length - 1; i > at; i--) {
      if (this.list[i][0].toLowerCase() === lower) this.list.splice(i, 1);
    }
    return this;
  }

  /** Removes every value of `name`. */
  delete(name: string): this {
    const lower = name.toLowerCase();
    for (let i = this.list.length - 1; i >= 0; i--) {
      if (this.list[i][0].toLowerCase() === lower) this.list.splice(i, 1);
    }
    return this;
  }

  /** Every pair, in order (a copy). */
  entries(): [string, string][] {
    return this.list.map(([n, v]) => [n, v]);
  }

  get length(): number {
    return this.list.length;
  }

  [Symbol.iterator](): Iterator<[string, string]> {
    return this.entries()[Symbol.iterator]();
  }
}

/** `init` as pairs, in order (a copy); a record's list value is one pair per
 * value. Names are kept as given. */
export function pairsOf(init?: HeadersInit): [string, string][] {
  if (init === undefined || init === null) return [];
  if (init instanceof HeaderList) return init.entries();
  if (Array.isArray(init)) return (init as ReadonlyArray<readonly [string, string]>).map(([n, v]) => [n, v]);
  const out: [string, string][] = [];
  for (const [name, value] of Object.entries(init as Record<string, string | readonly string[]>)) {
    if (typeof value === "string") out.push([name, value]);
    else for (const v of value) out.push([name, v]);
  }
  return out;
}

/** The headers of a response (`headers`, then `more`): names in lower case, repeats kept in order,
 * and `content-type: text/plain; charset=utf-8` unless one is given. */
export function responsePairs(headers?: HeadersInit, more?: HeadersInit): [string, string][] {
  const given = [...pairsOf(headers), ...pairsOf(more)].map(([n, v]) => [n.toLowerCase(), v] as [string, string]);
  return given.some(([n]) => n === "content-type") ? given : [["content-type", "text/plain; charset=utf-8"], ...given];
}

/** Pairs of bytes from the host (`fields.entries()`) as pairs of text. */
export function fieldsToPairs(entries: ReadonlyArray<readonly [string, Uint8Array]>): [string, string][] {
  const decoder = new TextDecoder();
  return entries.map(([n, v]) => [n, decoder.decode(v)]);
}

/** The record of `pairs` for the older `headers` properties: names in lower
 * case; repeats of a header are joined with `, ` (as `fetch` does), except
 * `set-cookie`, which stays its first value (read them all through the
 * list: a cookie's own commas make joining it ambiguous). */
export function fetchedRecord(pairs: ReadonlyArray<readonly [string, string]>): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [name, value] of pairs) {
    const k = name.toLowerCase();
    if (out[k] === undefined) out[k] = value;
    else if (k !== "set-cookie") out[k] = `${out[k]}, ${value}`;
  }
  return out;
}
