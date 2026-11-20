// A model of room, pusher and edges (ADR 0040, section 13) under lost
// pulls, lost and duplicated pushes, lost answers, room restarts (epoch
// bumps) and pusher restarts. Invariant: every socket sees every (e, n)
// once, in order, and eventually the head.
import { test } from "node:test";
import assert from "node:assert/strict";
import { Core } from "../src/broadcast-core";
import { serve } from "../src/broadcast";
import { Pulled } from "../src/broadcast-pusher";
import { Round } from "../src/broadcast-round";
import { Edge, type Push } from "../src/room-edge";
import * as db from "./directory-storage-stub";
import { install } from "./host-stub";
const PUSHER = "tenant/pusher/r";
function random(seed: number) {
    return () => {
        seed = (seed + 0x6d2b79f5) | 0;
        let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
        t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
        return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
}
interface Socket { last?: { e: number; n: number }; seen: number; errors: string[] }
function model(seed: number) {
    const rand = random(seed);
    db.install(":memory:");
    let core = new Core(db);
    core.init();
    const names = ["edge/a", "edge/b", "edge/c"];
    const sockets = new Map<string, Socket>();
    const edges = new Map(names.map(name => {
        const edge: Edge = new Edge({
            send(id, frame) {
                const s = sockets.get(`${name}#${id}`)!, f = JSON.parse(frame);
                if (f.t === "ready") {
                    if (f.n !== 0 && f.n !== null)
                        s.errors.push(`ready at ${f.n}`);
                    s.last = { e: f.e, n: 0 };
                }
                else if (f.t === "m")
                    f.m.forEach((_m: unknown, i: number) => {
                        const n = f.n + i;
                        // The client's rule 1 (section 6): a repeat in the same epoch is
                        // dropped. An edge repeats after a lost answer and an epoch bump:
                        // the pusher's stale position gives a lower base than the edge's.
                        if (s.last && f.e === s.last.e && n <= s.last.n) {
                            repeats++;
                            return;
                        }
                        if (!s.last || n !== s.last.n + 1 || f.e < s.last.e)
                            s.errors.push(`${f.e}.${n} after ${s.last?.e}.${s.last?.n}`);
                        s.last = { e: f.e, n };
                        s.seen++;
                    });
                else
                    s.errors.push(`unexpected ${frame}`);
                return "ok";
            },
            close(id, code) { sockets.get(`${name}#${id}`)!.errors.push(`closed ${code}`); },
        });
        return [name, edge] as const;
    }));
    let faults = true, repeats = 0;
    const trace: string[] = [];
    const chance = (p: number) => faults && rand() < p;
    install((address, r) => {
        if (address === "room/r") {
            // A lost pull: the call fails, the round defers its children.
            if (chance(0.15))
                throw { payload: { tag: "timeout" } };
            try {
                return { status: 200, headers: [], body: serve(core, r.path.slice("/wasmkeep-room/v1/".length), r.body, PUSHER, PUSHER) };
            }
            catch (e) {
                return { status: 503, headers: [], body: new Uint8Array() };
            }
        }
        const edge = edges.get(address)!;
        if (r.path.endsWith("/init"))
            return { status: 200, headers: [], body: new TextEncoder().encode("{}") };
        // A lost push never reaches the edge.
        if (chance(0.1))
            return { status: 503, headers: [], body: new Uint8Array() };
        const p: Push = JSON.parse(new TextDecoder().decode(r.body));
        trace.push(`${address} push e${p.e} b${p.b} first${p.first} head${p.head} [${p.messages.map((m: any) => `${m.e}.`).join("")}] edge-head ${edge.head?.e}.${edge.head?.n}`);
        let answer = edge.push(p);
        // A duplicated push: the same request applied twice.
        if (chance(0.2))
            answer = edge.push(p);
        // A lost answer: the edge applied it, the round does not know.
        if (chance(0.1))
            return { status: 503, headers: [], body: new Uint8Array() };
        return { status: 200, headers: [], body: new TextEncoder().encode(JSON.stringify(answer)) };
    });
    const source = new Pulled("room/r");
    let round = new Round(names), now = 0;
    // The edges learn a head, then the sockets open at it.
    faults = false;
    round.run(source, now += 20);
    round.run(source, now += 20);
    faults = true;
    let id = 0n;
    for (const [name, edge] of edges)
        for (let i = 0; i < 2; i++) {
            sockets.set(`${name}#${++id}`, { seen: 0, errors: [] });
            edge.open(id);
        }
    let published = 0;
    for (let step = 0; step < 150; step++) {
        const x = rand();
        if (x < 0.4) {
            const k = 1 + Math.floor(rand() * 3);
            core.publish(Array.from({ length: k }, (_, i) => ({ body: `m${published + i}` })), BigInt(now));
            published += k;
        }
        else if (x < 0.45)
            core = new Core(db); // A clean restart: the next operation opens an epoch.
        else if (x < 0.5)
            round = new Round(names); // A pusher restart: positions unknown.
        else
            round.run(source, now);
        now += 20 + Math.floor(rand() * 600);
    }
    // No more faults: the rounds converge.
    faults = false;
    const head = core.pull(undefined, 0, false, now).pull;
    for (let i = 0; i < 40 && [...sockets.values()].some(s => s.last?.n !== Number(head.head)); i++) {
        round.run(source, now);
        now += 17000; // Past any suspect's backoff.
    }
    for (const [key, s] of sockets) {
        assert.deepEqual(s.errors, [], `seed ${seed}, socket ${key}\n${trace.filter(t => t.startsWith(key.split("#")[0])).slice(-30).join("\n")}`);
        assert.equal(s.last?.n, Number(head.head), `seed ${seed}, socket ${key} ends at the head`);
        assert.equal(s.seen, Number(head.head), `seed ${seed}, socket ${key} saw each number once`);
    }
    db.close();
    return { published, repeats };
}
test("room, pusher and edges: every socket sees every (e, n) once, in order, eventually", () => {
    let total = 0, repeats = 0;
    for (let seed = 1; seed <= 40; seed++)
        { const r = model(seed); total += r.published; repeats += r.repeats; }
    assert(total > 1000);
    console.log(`model: ${total} messages, ${repeats} edge repeats dropped by the client rule`);
});
