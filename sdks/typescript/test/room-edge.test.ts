import { test } from "node:test";
import assert from "node:assert/strict";
import { Edge, type Push } from "../src/room-edge";
const push = (n: number): Push => ({ e: 1, b: 0, first: n, head: n, messages: [{ a: 0, d: "hello" }] });
test("edge cursor, repeats, bounded buffer, behind then slow close", () => {
    const frames: string[] = [], closed: number[] = [];
    let blocked = false;
    const edge = new Edge({ send(_id, text) { frames.push(text); return blocked ? "backpressure" : "ok"; }, close(_id, code) { closed.push(code); } });
    edge.push({ e: 1, b: 0, first: 1, head: 0, messages: [] });
    edge.open(1n);
    edge.push(push(1));
    const count = frames.length;
    edge.push(push(1));
    assert.equal(frames.length, count);
    blocked = true;
    for (let n = 2; n <= 514; n++)
        edge.push(push(n));
    assert.equal(edge.buffer.length, 512);
    assert.equal(JSON.parse(frames.at(-1)!).t, "behind");
    assert.deepEqual(closed, [4802]);
});
test("epoch rollback replaces reused numbers and holes signal behind", () => {
    const frames: string[] = [];
    const edge = new Edge({ send(_id, s) { frames.push(s); return "ok"; }, close() { } });
    edge.push({ e: 1, b: 0, first: 1, head: 0, messages: [] });
    edge.open(1n);
    edge.push(push(1));
    edge.push(push(2));
    edge.push({ e: 2, b: 1, first: 2, head: 2, messages: [{ a: 0, d: "replacement" }] });
    assert.equal(JSON.parse(frames.at(-1)!).e, 2);
    edge.push({ e: 2, b: 1, first: 5, head: 5, messages: [{ a: 0, d: "hole" }] });
    assert.equal(JSON.parse(frames.at(-1)!).t, "behind");
});
test("retained R2 message identities survive a newer push header", () => {
    const frames: any[] = [];
    const edge = new Edge({ send(_id, s) { frames.push(JSON.parse(s)); return "ok"; }, close() { } });
    edge.push({ e: 1, b: 0, first: 1, head: 0, messages: [] });
    edge.open(1n);
    edge.push({ e: 2, b: 1, first: 1, head: 2, messages: [{ a: 0, d: "old", e: 1, b: 0 }, { a: 0, d: "new", e: 2, b: 1 }] } as unknown as Push);
    edge.ping(1n);
    assert.deepEqual(frames.filter(f => f.t === "m").map(f => [f.e, f.b, f.n]), [[1, 0, 1], [2, 1, 2]]);
});
test("cap, cold adoption, byte-bound frames and successful behind recovery", () => {
    const frames: any[] = [];
    const edge = new Edge({ send(id, s) { frames.push([id, JSON.parse(s), new TextEncoder().encode(s).length]); return "ok"; }, close() { } });
    edge.push({ e: 1, b: 0, first: 1, head: 0, messages: [] });
    for (let i = 0; i < 500; i++)
        assert(edge.open(BigInt(i)));
    assert.equal(edge.open(500n), false);
    edge.push({ e: 1, b: 0, first: 1, head: 1, messages: [{ a: 0, d: '"'.repeat(65536) }] });
    assert(frames.every(f => f[2] <= 65536));
    assert.equal(frames.at(-1)[1].t, "behind");
    edge.push(push(2));
    assert.equal(frames.at(-1)[1].n, 2);
    const cold = new Edge({ send(_id, s) { frames.push([0, JSON.parse(s), s.length]); return "ok"; }, close() { } });
    cold.push({ e: 1, b: 0, first: 3, head: 2, messages: [] });
    cold.adopt([1n]);
    cold.ping(1n);
    assert.equal(frames.at(-1)[1].n, 2);
});
test("internal epoch metadata does not accumulate phantom buffer bytes", () => {
    const io = { send() { return "ok" as const; }, close() {} };
    const plain = new Edge(io), tagged = new Edge(io);
    for (let n = 1; n <= 800; n++) {
        const p = { e: 1, b: 0, first: n, head: n, messages: [{ a: 0, d: "x".repeat(8192) }] };
        plain.push(p);
        tagged.push({ ...p, messages: p.messages.map(m => ({ ...m, e: 1, b: 0 })) });
    }
    assert.equal(tagged.floor, plain.floor);
    assert.equal(tagged.buffer.length, plain.buffer.length);
});
test("an explicit empty history hole sends behind", () => {
    const frames: any[] = [];
    const edge = new Edge({ send(_id, s) { frames.push(JSON.parse(s)); return "ok"; }, close() {} });
    edge.push({e:1,b:0,first:1,head:0,messages:[]}); edge.open(1n);
    edge.push({e:70,b:1,first:2,head:1,messages:[],recover:true} as unknown as Push);
    assert.equal(frames.at(-1).t,"behind"); assert.equal(frames.at(-1).n,1);
});
