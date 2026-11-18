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
