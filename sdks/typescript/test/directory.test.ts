import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { create, change, lookup, point, DirectoryError } from "../src/directory-state";
const owner = "app/owner";
const initial = () => create(owner, "edge/a", new Uint8Array([1]));
const split = (lo: bigint, at: bigint, expect: bigint, op: string) => ({ kind: "split" as const, lo, at, target: "edge/b", meta: new Uint8Array([2]), expect, op });
test("split, merge, versions, replay and conflicts", () => {
    const a = initial(), b = change(a, owner, split(0n, 1n << 63n, 1n, "s"));
    assert.equal(lookup(a.map, (1n << 64n) - 1n).target, "edge/a");
    assert.deepEqual(b.map.entries.map(e => e.version), [2n, 2n]);
    assert.deepEqual(change(b, owner, split(0n, 1n << 63n, 1n, "s")), b);
    assert.throws(() => change(b, owner, split(0n, 10n, 1n, "other")), e => e instanceof DirectoryError && e.version === 2n);
    for (const keep of ["left", "right"] as const) {
        const c = change(b, owner, { kind: "merge", lo: 1n << 63n, keep, expect: 2n, op: "m" });
        assert.equal(c.map.entries.length, 1);
        assert.equal(c.map.entries[0].hi, undefined);
        assert.equal(c.map.entries[0].version, 3n);
        assert.equal(c.map.entries[0].target, keep === "left" ? "edge/a" : "edge/b");
    }
});
test("ownership, limits, boundaries and arbitrary sequence coverage", () => {
    let a = initial();
    assert.throws(() => change(a, "other", split(0n, 1n, 1n, "x")));
    assert.throws(() => change(a, owner, split(0n, 0n, 1n, "x")));
    assert.throws(() => create(owner, "edge/a", new Uint8Array(1025)));
    a.map = { version: 4096n, entries: Array.from({ length: 4096 }, (_, i) => ({ lo: BigInt(i), hi: i === 4095 ? undefined : BigInt(i + 1), target: "edge/a", version: 1n, meta: new Uint8Array() })) };
    assert.throws(() => change(a, owner, split(4095n, 4096n, 4096n, "full")));
    a = initial();
    let seed = 17n;
    for (let n = 0; n < 300; n++) {
        seed = BigInt.asUintN(64, seed * 6364136223846793005n + 1n);
        const e = lookup(a.map, seed), before = a;
        a = change(a, owner, n % 3 === 0 && a.map.entries.length > 1 ? { kind: "merge", lo: a.map.entries[1].lo, keep: "right", expect: a.map.version, op: String(n) } : split(e.lo, seed, a.map.version, String(n)));
        assert.deepEqual(lookup(before.map, seed), e);
        assert.equal(a.map.entries[0].lo, 0n);
        assert.equal(a.map.entries.at(-1)?.hi, undefined);
        for (let i = 1; i < a.map.entries.length; i++) {
            assert.equal(a.map.entries[i - 1].hi, a.map.entries[i].lo);
            assert(a.map.entries[i - 1].lo < a.map.entries[i].lo);
        }
    }
});
test("shared point vectors", () => {
    const text = readFileSync("../../crates/wasmkeep-sdk/src/directory/point-vectors.txt", "utf8");
    for (const line of text.trimEnd().split("\n")) {
        const [hex, expected] = line.split(" ");
        assert.equal(point(Uint8Array.from(hex.match(/../g) ?? [], b => parseInt(b, 16))), BigInt("0x" + expected));
    }
});
test("unchanged versions, adjacent boundaries and old operation ids", () => {
    const a = change(initial(), owner, split(0n, 100n, 1n, "a"));
    const b = change(a, owner, split(100n, 200n, 2n, "b"));
    assert.deepEqual(b.map.entries.map(e => e.version), [2n, 3n, 3n]);
    assert.throws(() => change(b, owner, split(0n, 100n, 1n, "a")), e => e instanceof DirectoryError && e.version === 3n);
    assert.throws(() => change(b, owner, split(0n, 100n, 3n, "boundary")));
    assert.throws(() => change(b, owner, { kind: "merge", lo: 0n, keep: "left", expect: 3n, op: "zero" }));
    assert.doesNotThrow(() => create(owner, "edge/a", new Uint8Array(1024)));
});
