// A room's per-event work must not grow with what the room went through
// a full window, many lagging children and a scale
// operation each stay bounded per event, so no event exceeds the default
// CPU budget whatever state a heavy run left behind.
import { test } from "node:test";
import assert from "node:assert/strict";
import { Core, type Store } from "../src/broadcast-core";
import { Round } from "../src/broadcast-round";
import { ScaleHost } from "../src/broadcast-scaling";
import * as db from "./directory-storage-stub";
import * as alarms from "./room-alarms-stub";
import { install } from "./host-stub";
import type { Value } from "wasmkeep:actor/types@0.2.0";

/** The stub store, counting the rows and text bytes each query hands back. */
function counting() {
    const seen = { rows: 0, bytes: 0 };
    const store: Store = { ...db, query(s: string, p: Value[]) {
            const r = db.query(s, p);
            seen.rows += r.values.length;
            for (const row of r.values)
                for (const v of row)
                    if (v.tag === "text" || v.tag === "blob")
                        seen.bytes += v.val.length;
            return r;
        } };
    return { store, seen, reset() { seen.rows = 0; seen.bytes = 0; } };
}
function fill(count: number, size: number, from = 1) {
    db.exec("BEGIN", []);
    for (let n = from; n < from + count; n++)
        db.exec("INSERT INTO wasmkeep_room_history(n,e,at,id,body) VALUES(?1,1,0,?2,?3)", [{ tag: "integer", val: BigInt(n) }, { tag: "text", val: `id${n}` }, { tag: "text", val: "x".repeat(size) }]);
    db.exec("COMMIT", []);
}
const one = (sql: string) => Number((db.query(sql, []).values[0][0] as { val: bigint }).val);
test("a publish on a full window reads a bounded number of rows and keeps the window", () => {
    db.install(":memory:");
    const c = counting(), core = new Core(c.store, alarms);
    core.init();
    core.wake();
    fill(2000, 256);
    c.reset();
    assert.deepEqual(core.publish([{ body: "new", id: "fresh" }], 1n), [{ e: 1n, n: 2001n }]);
    assert(c.seen.rows <= 8, `publish read ${c.seen.rows} rows`);
    assert(c.seen.bytes <= 4096, `publish read ${c.seen.bytes} bytes`);
    assert.equal(one("SELECT count(*) FROM wasmkeep_room_history"), 2000);
    assert.equal(one("SELECT min(n) FROM wasmkeep_room_history"), 2);
    // A retry under an id still in the window answers its number.
    c.reset();
    assert.deepEqual(core.publish([{ body: "again", id: "id1500" }], 2n), [{ e: 1n, n: 1500n }]);
    assert(c.seen.rows <= 8, `retry read ${c.seen.rows} rows`);
    // The byte window: 8 MiB, oldest first.
    for (let i = 0; i < 33; i++)
        core.publish(Array.from({ length: 4 }, () => ({ body: "z".repeat(65536) })), 3n);
    const bytes = one("SELECT sum(length(CAST(body AS BLOB))) FROM wasmkeep_room_history");
    assert(bytes <= 8 * 1024 * 1024 && bytes > 8 * 1024 * 1024 - 65536, `${bytes} bytes`);
    db.close();
});
test("history reads the page it answers, not the window", () => {
    db.install(":memory:");
    const c = counting(), core = new Core(c.store, alarms);
    core.init();
    core.wake();
    fill(2000, 1024);
    c.reset();
    const h = core.history({ e: 1n, n: 0n });
    assert(h.more && h.messages.length > 200 && h.messages.length < 260, `${h.messages.length}`);
    assert(c.seen.rows <= h.messages.length + 80, `history read ${c.seen.rows} rows for ${h.messages.length}`);
    c.reset();
    assert.equal(core.history(undefined, 2000, 3).messages.map(m => m.position.n).join(), "1998,1999,2000");
    assert(c.seen.rows <= 80, `last=3 read ${c.seen.rows} rows`);
    db.close();
});
test("a round with many lagging children builds at most 1 MiB, then continues 20 ms later", () => {
    db.install(":memory:");
    const c = counting(), core = new Core(c.store, alarms);
    core.init();
    core.wake();
    fill(2000, 2048);
    const edges = Array.from({ length: 16 }, (_, i) => `edge/e${i}`);
    const at = new Map(edges.map((e, i) => [e, i * 100]));
    let bytes = 0, calls = 0;
    install((address, r) => {
        calls++;
        bytes += r.body.length;
        const p = JSON.parse(new TextDecoder().decode(r.body));
        if (p.messages?.length)
            at.set(address, p.first + p.messages.length - 1);
        return { status: 200, headers: [], body: new TextEncoder().encode(JSON.stringify({ e: 1, n: at.get(address), lagging: 0 })) };
    });
    const round = new Round(edges);
    round.run(core, 0); // init
    round.run(core, 20); // header only: the positions
    let now = 40, rounds = 0;
    while ([...at.values()].some(n => n < 2000)) {
        bytes = 0; calls = 0; c.reset();
        round.run(core, now);
        assert(bytes <= 1024 * 1024 + 16 * 128, `round ${rounds} built ${bytes} bytes in ${calls} calls`);
        assert(c.seen.bytes <= 2 * 1024 * 1024, `round ${rounds} read ${c.seen.bytes} bytes`);
        if ([...at.values()].some(n => n < 2000))
            assert.equal(alarms.pending, BigInt(now + 20));
        now += 20;
        assert(++rounds < 64, "lagging children must catch up");
    }
    db.close();
});
test("a scale pass runs one durable step per event and continues on the alarm", () => {
    db.install(":memory:");
    const host = new ScaleHost({ directory: "directory/bg:r", edgeClass: "edge", capacity: 10 });
    host.init();
    const core = new Core(db, alarms);
    core.init();
    const effects: string[] = [];
    install((address, r) => {
        if (!r.path.endsWith("/push"))
            effects.push(`${address} ${r.path}`);
        return { status: r.path.endsWith("/create") ? 503 : 200, headers: [], body: new TextEncoder().encode(r.path.endsWith("/push") ? '{"e":1,"n":0,"sockets":0,"lagging":0}' : '{"sockets":0}') };
    });
    // The first event boots the group by one step; the directory's step is the next event's.
    host.run(new Round([]), core, 0);
    assert.deepEqual(effects, ["edge/r.e1 /wasmkeep-edge/v1/init"]);
    assert.equal(host.load()!.op!.step, 1);
    assert.equal(alarms.pending, 0n);
    db.close();
});
import { useSqlite, encode, decode, byteLength, fits } from "../src/room-utf8";
test("the SQLite UTF-8 path answers exactly what TextEncoder and TextDecoder do", () => {
    db.install(":memory:");
    useSqlite(true);
    try {
        const big = (s: string) => s.repeat(Math.ceil(2048 / s.length));
        for (const s of [big("x"), big("é漢😀"), "﻿" + big("a\"\\\n"), big("a") + "\ud800" + "b", big("\udc00x")]) {
            assert.deepEqual(encode(s), new TextEncoder().encode(s));
            assert.equal(byteLength(s), new TextEncoder().encode(s).length);
            assert.equal(fits(s, 65536), new TextEncoder().encode(s).length <= 65536);
        }
        const valid = new TextEncoder().encode("﻿" + big("é漢😀"));
        assert.equal(decode(valid), new TextDecoder("utf-8", { ignoreBOM: true }).decode(valid));
        assert.equal(decode(valid, { stripBOM: true }), new TextDecoder().decode(valid));
        const invalid = Uint8Array.from([...valid, 0xff, 0xc3, 0x28, 0xe2, 0x82, ...valid]);
        assert.equal(decode(invalid), new TextDecoder("utf-8", { ignoreBOM: true }).decode(invalid));
        assert.throws(() => decode(invalid, { fatal: true }), TypeError);
        const replacement = new TextEncoder().encode(big("�"));
        assert.equal(decode(replacement, { fatal: true }), new TextDecoder().decode(replacement));
    }
    finally {
        useSqlite(undefined);
        db.close();
    }
});
/** Edges answering what they got: a push past their position plus one is a
 * hole, after which the edge holds the push's last number (and tells its
 * sockets they are behind). */
function edges(_names: string[], at: Map<string, number>, pushed: Map<string, number>, holes: Set<string>) {
    return (address: string, p: any) => {
        if (p.messages?.length) {
            pushed.set(address, (pushed.get(address) ?? 0) + 1);
            if (p.first > at.get(address)! + 1)
                holes.add(address);
            at.set(address, p.first + p.messages.length - 1);
        }
        return { e: 1, n: at.get(address), lagging: 0 };
    };
}
test("sustained 256 KiB batches never starve a child: every child advances, one that falls out of the window is told", () => {
    db.install(":memory:");
    const core = new Core(db, alarms);
    core.init();
    core.wake();
    const names = ["edge/a", "edge/b", "edge/c", "edge/d"];
    const at = new Map(names.map(n => [n, 0])), pushed = new Map<string, number>(), holes = new Set<string>();
    const answer = edges(names, at, pushed, holes);
    install((address, r) => ({ status: 200, headers: [], body: new TextEncoder().encode(JSON.stringify(answer(address, JSON.parse(new TextDecoder().decode(r.body))))) }));
    const round = new Round(names);
    round.run(core, 0);
    round.run(core, 20);
    let now = 40;
    for (let i = 0; i < 60; i++) {
        core.publish(Array.from({ length: 4 }, () => ({ body: "z".repeat(65536) })), BigInt(now));
        round.run(core, now);
        now += 20;
    }
    const head = Number(core.last());
    for (const n of names) {
        // 1 MiB a round serves about four 256 KiB slices: each child at least every fourth round.
        assert((pushed.get(n) ?? 0) >= 15, `${n} pushed ${pushed.get(n) ?? 0} times in 60 rounds: ${[...pushed]}`);
        assert(head - at.get(n)! <= 128, `${n} at ${at.get(n)} of ${head}`);
    }
    db.close();
});
test("a child left behind the window gets a push past its position (the edge's behind)", () => {
    db.install(":memory:");
    const core = new Core(db, alarms);
    core.init();
    core.wake();
    fill(100, 10, 1000);
    const at = new Map([["edge/old", 5]]), pushed = new Map<string, number>(), holes = new Set<string>();
    const answer = edges(["edge/old"], at, pushed, holes);
    install((address, r) => ({ status: 200, headers: [], body: new TextEncoder().encode(JSON.stringify(answer(address, JSON.parse(new TextDecoder().decode(r.body))))) }));
    const round = new Round(["edge/old"]);
    round.run(core, 0);
    round.run(core, 20);
    round.run(core, 40);
    assert(holes.has("edge/old"));
    assert.equal(at.get("edge/old"), 1099);
    db.close();
});
test("a round reads a bounded number of rows whatever the children's positions, empty bodies included", () => {
    db.install(":memory:");
    const c = counting(), core = new Core(c.store, alarms);
    core.init();
    core.wake();
    db.exec("BEGIN", []);
    for (let n = 1; n <= 2000; n++)
        db.exec("INSERT INTO wasmkeep_room_history(n,e,at,id,body) VALUES(?1,1,0,?2,'')", [{ tag: "integer", val: BigInt(n) }, { tag: "text", val: "i".repeat(60) + String(n).padStart(4, "0") }]);
    db.exec("COMMIT", []);
    const children = Array.from({ length: 256 }, (_, i) => ({ name: `edge/r.e${i}` }));
    const relays = Array.from({ length: 16 }, (_, r) => ({ name: `relay/r.r${r}`, edges: children.slice(r * 16, r * 16 + 16).map(c => c.name) }));
    // Each child stays at its own position: 256 distinct slices every round.
    const position = (name: string) => Number(name.split(".e")[1]) * 7;
    install((_address, r) => {
        const pack = JSON.parse(new TextDecoder().decode(r.body));
        return { status: 200, headers: [], body: new TextEncoder().encode(JSON.stringify(pack.children.map((ch: any) => ({ e: 1, n: position(ch.edge), lagging: 0 })))) };
    });
    const round = new Round([]);
    round.sync(children, relays);
    round.run(core, 0); // header only: the positions
    for (let i = 1; i <= 3; i++) {
        c.reset();
        round.run(core, i * 20);
        assert(c.seen.rows <= 1100, `round ${i} read ${c.seen.rows} rows`);
    }
    db.close();
});
