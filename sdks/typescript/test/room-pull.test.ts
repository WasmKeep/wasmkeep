// ADR 0040, section 13: the room's pull, shared with the Rust SDK through
// pull-vectors.txt, through Core and through the wire (handler and Pulled).
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { Core, type Pull, type Store } from "../src/broadcast-core";
import { define } from "../src/broadcast";
import { Pulled } from "../src/broadcast-pusher";
import { Client } from "../src/broadcast-client";
import * as db from "./directory-storage-stub";
import { install } from "./host-stub";
import type { Value } from "wasmkeep:actor/types@0.2.0";
import type { ActorRequest } from "../src/request";
import { toBytes, Writer } from "../src/wire";

/** The room's handler, called as `caller`. */
function handler(caller = "tenant/pusher/r") {
    const actor = define();
    actor.init!();
    return (path: string, body: Uint8Array, as = caller) => {
        const req: ActorRequest = { method: "POST", path, headers: { "x-wasmkeep-caller": as }, bytes: () => body, text: () => "", json: <T>() => ({}) as T };
        const r = actor.fetch!(req);
        assert(typeof r === "object" && !(r instanceof Uint8Array));
        return { status: r.status ?? 200, headers: [] as [string, string][], body: typeof r.body === "string" ? toBytes(r.body) : r.body ?? new Uint8Array() };
    };
}
type Mode = "core" | "wire";
function vectors(mode: Mode) {
    const text = readFileSync("../../crates/wasmkeep-sdk/src/broadcast/pull-vectors.txt", "utf8");
    const scenarios = text.split(/\n\s*\n/).map(s => s.split("\n").filter(l => l.trim() && !l.startsWith("#"))).filter(s => s.length);
    assert(scenarios.length >= 5);
    for (const lines of scenarios) {
        db.install(":memory:");
        let core = new Core(db), now = 0, serve = handler();
        core.init();
        const pulled = new Pulled("room/r");
        install((_a, r) => serve(r.path, r.body));
        const publish = (body: string) => mode === "core" ? core.publish([{ body }], BigInt(now)) : new Client("room/r").publish([{ body }]);
        for (const line of lines) {
            const [cmd, ...a] = line.split(" ");
            if (cmd === "publish")
                for (let i = 0; i < Number(a[0]); i++)
                    publish("x".repeat(Number(a[1] ?? 1)));
            else if (cmd === "lose")
                db.exec("DELETE FROM wasmkeep_room_history WHERE n>?1", [{ tag: "integer", val: BigInt(a[0]) }]);
            else if (cmd === "trim")
                db.exec("DELETE FROM wasmkeep_room_history WHERE n<=?1", [{ tag: "integer", val: BigInt(a[0]) }]);
            else if (cmd === "epochdrop")
                db.exec("DELETE FROM wasmkeep_room_epochs WHERE e=?1", [{ tag: "integer", val: BigInt(a[0]) }]);
            else if (cmd === "restart") {
                core = new Core(db);
                serve = handler();
            }
            else if (cmd === "clock")
                now = Number(a[0]);
            else if (cmd === "poke") {
                // Through the wire the room's handler has its own Core and clock:
                // the publish answer's flag is checked by the next test.
                if (mode === "core")
                    assert.equal(core.poke(now) ? 1 : 0, Number(a[0]), `${line} (${mode})`);
            }
            else if (cmd === "pull") {
                const after = a[0] === "-" ? undefined : { e: BigInt(a[0]), n: BigInt(a[1]) };
                const budget = Number(a[2]), idle = a[3] === "1";
                const p: Pull = mode === "core" ? core.pull(after, budget, idle, now).pull : pulled.pull(after, budget, idle);
                const last = p.messages.at(-1);
                const got = [p.e, p.b, p.first, p.head, p.recover ? 1n : 0n, BigInt(p.messages.length), last?.position.e ?? 0n, last?.b ?? 0n].join(" ");
                assert.equal(got, a.slice(5).join(" "), `${line} (${mode})`);
                p.messages.forEach((m, i) => assert.equal(m.position.n, p.first + BigInt(i)));
            }
            else
                throw Error(`unknown vector command ${line}`);
        }
        db.close();
    }
}
test("shared pull vectors through Core", () => vectors("core"));
test("shared pull vectors through the room's handler and the pusher's Pulled", () => vectors("wire"));
test("the publish answer asks for a poke only while the pusher is idle", () => {
    // Through the wire the room's handler keeps its own Core; the vectors'
    // pokes are checked on the publish answer instead.
    db.install(":memory:");
    const serve = handler();
    install((_a, r) => serve(r.path, r.body));
    const pulled = new Pulled("room/r"), client = new Client("room/r");
    const pokes: string[] = [];
    install((a, r) => {
        if (a === "pusher/r") {
            pokes.push(new TextDecoder().decode(r.body));
            return { status: 200, headers: [], body: new Uint8Array() };
        }
        return serve(r.path, r.body);
    });
    client.publish([{ body: "x".repeat(60000) }]);
    assert.equal(pokes.length, 1, "a cold room asks for a poke");
    const p = pulled.pull({ e: 1n, n: 0n }, 1 << 20, false);
    assert.deepEqual([p.e, p.b, p.first, p.head, p.messages.length, p.messages[0].bodyBytes, p.messages[0].body.length], [1n, 0n, 1n, 1n, 1, 60000, 60000]);
    client.publish([{ body: "é" }]);
    assert.equal(pokes.length, 1, "a busy pusher is not poked");
    const q = pulled.pull({ e: 1n, n: 1n }, 1 << 20, false);
    assert.deepEqual([q.messages[0].body, q.messages[0].bodyBytes], ["é", 2]);
    pulled.pull({ e: 1n, n: 2n }, 0, true);
    client.publish([{ body: "z" }]);
    assert.equal(pokes.length, 2, "an idle pusher is poked");
    db.close();
});
test("pull and adopt answer the room's pusher only", () => {
    db.install(":memory:");
    const serve = handler();
    const pull = new Writer().u64(0).u64(0).u64(0).finish();
    for (const caller of ["tenant/front/main", "tenant/edge/r.e1", "tenant/pusher/other"]) {
        assert.equal(serve("/wasmkeep-room/v1/pull", pull, caller).status, 403);
        assert.equal(serve("/wasmkeep-room/v1/adopt", new Writer().u64(0).finish(), caller).status, 403);
    }
    assert.equal(serve("/wasmkeep-room/v1/pull", pull).status, 200);
    db.close();
});
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
test("a pull reads what it answers, never the window, whatever the position", () => {
    db.install(":memory:");
    const c = counting(), core = new Core(c.store);
    core.init();
    core.wake();
    db.exec("BEGIN", []);
    for (let n = 1; n <= 2000; n++)
        db.exec("INSERT INTO wasmkeep_room_history(n,e,at,id,body) VALUES(?1,1,0,NULL,?2)", [{ tag: "integer", val: BigInt(n) }, { tag: "text", val: n % 2 ? "" : "x".repeat(4096) }]);
    db.exec("COMMIT", []);
    for (const [after, budget] of [[0n, 1 << 20], [999n, 1 << 20], [1990n, 1 << 20], [0n, 0], [1500n, 1], [2000n, 1 << 20]] as const) {
        c.reset();
        const { pull: p } = core.pull({ e: 1n, n: after }, budget, false, 0, true);
        assert(p.messages.length <= 513);
        // The rows it answers, one page of lookahead, and the head and epochs.
        assert(c.seen.rows <= p.messages.length + 32 + 70, `pull after ${after} read ${c.seen.rows} rows for ${p.messages.length}`);
        const bytes = p.messages.reduce((n, m) => n + m.bodyBytes, 0);
        assert(bytes <= 512 * 1024 + 65536, `${bytes} bytes`);
        assert(c.seen.bytes <= bytes + 33 * 4096 + 4096, `pull after ${after} read ${c.seen.bytes} bytes for ${bytes}`);
    }
    assert.throws(() => core.pull({ e: 1n, n: 0n }, (1 << 20) + 1, false, 0), /invalid pull budget/);
    assert.throws(() => core.pull({ e: 1n, n: 2001n }, 0, false, 0), /invalid position/);
    assert.throws(() => core.pull({ e: 2n, n: 0n }, 0, false, 0), /invalid position/);
    db.close();
});
