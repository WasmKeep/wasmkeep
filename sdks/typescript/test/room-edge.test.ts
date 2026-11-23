import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
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
test("a push budgets distinct cursor frames and prioritizes caught-up sockets", () => {
    let blocked = true;
    const sent: bigint[] = [];
    const edge = new Edge({ send(id, text) { if (JSON.parse(text).t === 'm') sent.push(id); return blocked ? 'backpressure' : 'ok'; }, close() {} });
    const messages = Array.from({length: 32}, () => ({a:0,d:'é'.repeat(1000)}));
    edge.push({e:1,b:0,first:1,head:32,messages});
    for (let n=0; n<20; n++) edge.open(BigInt(n), {e:1,n});
    edge.open(100n); edge.open(101n);
    blocked = false; sent.length = 0;
    const ack = edge.push({e:1,b:0,first:33,head:33,messages:[{a:0,d:'healthy'}]});
    assert.deepEqual(sent.slice(0,2), [100n,101n]);
    assert.equal(sent.length, 17); // healthy group + 15 other distinct cursors
    assert.equal(ack.lagging, 5);
    sent.length = 0;
    assert.equal(edge.push({e:1,b:0,first:34,head:33,messages:[]}).lagging, 0);
    assert.equal(sent.length, 5);
});
// A frame in the notation of wake-vectors.txt, shared with Rust.
const notation = (id: bigint, frame: string) => {
    const f = JSON.parse(frame);
    if (f.t === "ready")
        return f.b !== undefined ? `${id} ready ${f.e} ${f.b} ${f.n}` : `${id} ready ${f.e} ${f.n}`;
    if (f.t === "behind")
        return `${id} behind ${f.e} ${f.n}`;
    if (f.t === "pong")
        return f.n === null || f.n === undefined ? `${id} pong null` : `${id} pong ${f.e} ${f.n}`;
    if (f.t === "m")
        return `${id} m ${f.e} ${f.b} ${f.n} ${f.m.length}`;
    throw Error(frame);
};
test("shared wake vectors: a woken edge's first push reaches its sockets", () => {
    const text = readFileSync("../../crates/wasmkeep-sdk/src/broadcast/wake-vectors.txt", "utf8");
    let count = 0;
    for (const block of text.split("\n\n")) {
        const scenario = block.split("\n").filter(l => l && !l.startsWith("#"));
        if (!scenario.length)
            continue;
        count++;
        const sent: string[] = [];
        let sockets: bigint[] = [];
        const gone = new Set<bigint>();
        let counted: number | undefined;
        const edge = new Edge({
            send(id, s) {
                if (gone.has(id))
                    return "gone";
                sent.push(notation(id, s));
                return "ok";
            },
            close() { },
        });
        let expected: string[] = [], event = "";
        const check = () => {
            assert.deepEqual(sent.splice(0), expected, `after ${event} in:\n${scenario.join("\n")}`);
            expected = [];
        };
        for (const line of scenario) {
            const w = line.split(" ");
            if (/^[0-9]+$/.test(w[0])) {
                expected.push(line);
                continue;
            }
            if (event)
                check();
            event = line;
            const n = (i: number) => Number(w[i]);
            if (w[0] === "sockets")
                sockets = w[1].split(",").map(BigInt);
            else if (w[0] === "open") {
                sockets.push(BigInt(w[1]));
                assert(edge.open(BigInt(w[1]), w.length === 4 ? { e: n(2), n: n(3) } : undefined));
            }
            else if (w[0] === "ping")
                edge.ping(BigInt(w[1]));
            else if (w[0] === "stored")
                edge.stored({ e: n(1), n: n(2) });
            else if (w[0] === "gone")
                gone.add(BigInt(w[1]));
            else if (w[0] === "count")
                assert.equal(counted, n(1), line);
            else if (w[0] === "push" || w[0] === "recover") {
                const p = w[0] === "recover"
                    ? { recover: true, e: n(1), b: n(2), first: n(3) + 1, head: n(3), messages: [] }
                    : { e: n(1), b: n(2), first: n(3), head: n(4), messages: Array.from({ length: n(5) }, (_, i) => w.length === 9 && i < n(8) ? { a: 0, d: "x", e: n(6), b: n(7) } : { a: 0, d: "x" }) };
                counted = edge.receive(p, [...sockets]).sockets;
                // A socket found gone leaves the host's list.
                sockets = sockets.filter(id => !gone.has(id));
            }
            else
                throw Error(`unknown event ${line}`);
        }
        check();
    }
    assert.equal(count, 14);
});
test("a wake announcement that meets backpressure closes the socket as slow", () => {
    const closed: [bigint, number][] = [];
    const edge = new Edge({ send() { return "backpressure"; }, close(id, code) { closed.push([id, code]); } });
    const ack = edge.receive({ ...push(5), first: 6, messages: [] }, [1n]);
    assert.deepEqual(closed, [[1n, 4802]]);
    assert.equal(ack.sockets, 0);
    assert.equal(ack.slow, 1);
});
