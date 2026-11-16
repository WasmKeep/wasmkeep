// The runtime codecs, contracts' request building, and path/query encoding.

import { test } from "node:test";
import assert from "node:assert/strict";
import * as codec from "../src/codec";
import { CodecError } from "../src/codec";
import { defineOperation, urlPath, withQuery } from "../src/contracts";

const bad = (c: { parse(v: unknown): unknown }, v: unknown) =>
  assert.throws(() => c.parse(v), (e: unknown) => e instanceof CodecError);

test("optional and nullable stay distinct", () => {
  const o = codec.object({ a: codec.optional(codec.string()), b: codec.nullable(codec.string()) });
  assert.deepEqual(o.parse({ b: null }), { b: null });
  assert.deepEqual(o.parse({ a: "x", b: "y" }), { a: "x", b: "y" });
  bad(o, { a: null, b: null }); // null is not absent
  bad(o, { a: "x" }); // nullable is not optional
  const both = codec.object({ c: codec.optional(codec.nullable(codec.integer())) });
  assert.deepEqual(both.parse({}), {});
  assert.deepEqual(both.parse({ c: null }), { c: null });
});

test("objects drop unknown keys, and refuse wrong shapes", () => {
  const o = codec.object({ n: codec.integer() });
  assert.deepEqual(o.parse({ n: 1, extra: true }), { n: 1 });
  bad(o, { n: 1.5 });
  bad(o, { n: "1" });
  bad(o, []);
  bad(o, null);
  bad(o, "x");
  assert.throws(() => codec.object({ ["__proto__"]: codec.string() }), TypeError as never);
  const parsed = codec.record(codec.integer()).parse(JSON.parse('{"__proto__":1}'));
  assert.equal(Object.getPrototypeOf(parsed), Object.prototype);
  assert.equal(Object.keys(parsed).length, 1);
});

test("errors say where", () => {
  const o = codec.object({ items: codec.array(codec.object({ sku: codec.string() })) });
  assert.throws(() => o.parse({ items: [{ sku: "a" }, { sku: 2 }] }), (e: unknown) => e instanceof CodecError && e.path === "items[1].sku");
});

test("scalars, literals, unions, arrays", () => {
  bad(codec.number(), Number.NaN);
  bad(codec.integer(), 2 ** 53);
  bad(codec.string({ maxLength: 2 }), "abc");
  bad(codec.array(codec.string(), { maxItems: 1 }), ["a", "b"]);
  bad(codec.oneOf(["a", "b"]), "c");
  bad(codec.literal(1), 2);
  bad(codec.null(), undefined);
  bad(codec.none(), 1);
  assert.equal(codec.none().parse(undefined), undefined);
  const u = codec.union([codec.string(), codec.integer()]);
  assert.equal(u.parse(3), 3);
  bad(u, true);
});

test("requests: JSON body for writes, none for reads, accept header always", () => {
  const dec = new TextDecoder();
  const add = defineOperation({
    name: "add",
    method: "post",
    path: "/items",
    input: codec.object({ sku: codec.string() }),
    output: codec.none(),
  });
  const r = add.request({ sku: "a b" });
  assert.equal(r.method, "POST");
  assert.equal(dec.decode(r.body), '{"sku":"a b"}');
  assert.equal(r.headers["content-type"], "application/json");
  assert.equal(r.headers.accept, "application/json");
  const get = defineOperation({
    name: "get",
    path: (i: { id: string }) => urlPath`/c/${i.id}`,
    input: codec.object({ id: codec.string() }),
  });
  const g = get.request({ id: "1" });
  assert.equal(g.method, "GET");
  assert.equal(g.body, undefined);
  assert.equal(g.path, "/c/1");
  // Input is validated before a request exists.
  assert.throws(() => add.request({ sku: 1 } as never), (e: unknown) => e instanceof CodecError);
  assert.throws(() => get.request(undefined as never), (e: unknown) => e instanceof CodecError);
});

test("path values are encoded as one safe segment", () => {
  assert.equal(urlPath`/a/${"x/y"}/b`, "/a/x%2Fy/b");
  assert.equal(urlPath`/a/${".."}`, "/a/%2E%2E");
  assert.equal(urlPath`/a/${"."}`, "/a/%2E");
  assert.equal(urlPath`/a/${"é ?#%"}`, "/a/%C3%A9%20%3F%23%25");
  assert.equal(urlPath`/a/${"(*)'!"}`, "/a/%28%2A%29%27%21");
  assert.equal(urlPath`/n/${5}/${true}`, "/n/5/true");
  assert.throws(() => urlPath`/a/${"\ud800"}`, (e: unknown) => e instanceof CodecError);
  assert.throws(() => urlPath`/a/${{}}`, (e: unknown) => e instanceof CodecError);
  assert.throws(() => urlPath`/a/${null}`, (e: unknown) => e instanceof CodecError);
});

test("query values are encoded; undefined is left out; arrays repeat", () => {
  assert.equal(withQuery("/s", {}), "/s");
  assert.equal(withQuery("/s", { q: undefined }), "/s");
  assert.equal(withQuery("/s", { q: "a&b=c", n: 2, t: ["x y", "z"] }), "/s?q=a%26b%3Dc&n=2&t=x%20y&t=z");
  assert.equal(withQuery("/s?a=1", { b: 2 }), "/s?a=1&b=2");
  assert.throws(() => withQuery("/s", { q: null as never }), (e: unknown) => e instanceof CodecError);
});

test("a path that is not a path is refused", () => {
  const mk = (p: string) => defineOperation({ name: "x", path: p });
  for (const p of ["a", "/a b", "/a#b", "/a\\b", "/a\nb"]) {
    assert.throws(() => mk(p).request(undefined), (e: unknown) => e instanceof CodecError, p);
  }
  assert.throws(() => defineOperation({ name: "", path: "/" }), TypeError as never);
  assert.throws(() => defineOperation({ name: "x", path: "/", method: "G ET" }), TypeError as never);
});
