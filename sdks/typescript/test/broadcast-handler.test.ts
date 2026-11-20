import { test } from "node:test";
import assert from "node:assert/strict";
import { install as host } from "./host-stub";
import * as db from "./directory-storage-stub";
import { define } from "../src/broadcast";
import { Client } from "../src/broadcast-client";
import type { ActorRequest } from "../src/request";
import { toBytes } from "../src/wire";
test("room front client and internal handler round trip, restart and access", () => {
    db.install(":memory:");
    let actor = define();
    actor.init!();
    const request = (path: string, body: Uint8Array, caller = true) => {
        const req: ActorRequest = { method: "POST", path, headers: caller ? { "x-wasmkeep-caller": "app/front" } : {}, bytes: () => body, text: () => "", json: <T>() => ({}) as T };
        const r = actor.fetch!(req);
        assert(typeof r === "object" && !(r instanceof Uint8Array));
        return { status: r.status ?? 200, headers: [], body: typeof r.body === "string" ? toBytes(r.body) : r.body ?? new Uint8Array() };
    };
    let pokes = 0;
    host((address, req) => {
        // A publish to an idle pusher pokes it (ADR 0040, section 13).
        if (address === "pusher/test") {
            pokes++;
            return { status: 200, headers: [], body: new Uint8Array() };
        }
        assert.equal(address, "room/test");
        return request(req.path, req.body);
    });
    const c = new Client("room/test");
    assert.equal(request("/wasmkeep-room/v1/history", new Uint8Array(), false).status, 403);
    assert.deepEqual(c.publish([{ body: "hello", id: "a" }, { body: "lost" }]), [{ e: 1n, n: 1n }, { e: 1n, n: 2n }]);
    assert.equal(db.commits, 1);
    c.history();
    assert.equal(db.commits, 1);
    db.exec("DELETE FROM wasmkeep_room_history WHERE n=2", []);
    actor = define();
    actor.init!();
    const h = c.history({ e: 1n, n: 2n });
    assert.deepEqual(h.lost, { e: 2n, base: 1n });
    assert.equal(h.messages.length, 0);
    assert.equal(db.commits, 2);
    assert.deepEqual(c.publish([{ body: "replacement" }]), [{ e: 2n, n: 2n }]);
    assert.equal(c.history({ e: 2n, n: 1n }).messages[0].body, "replacement");
    assert.equal(request("/wasmkeep-room/v1/publish", new Uint8Array([255])).status, 400);
    assert.deepEqual(c.publish([{ id: "x", body: "plain" }, { id: "\uFEFFx", body: "\uFEFFbody" }]), [{ e: 2n, n: 3n }, { e: 2n, n: 4n }]);
    assert.equal(pokes, 3, "no pull came: every publish asks for a poke");
    const unicode = c.history({ e: 2n, n: 2n }).messages;
    assert.deepEqual(unicode.map(m => [m.id, m.body]), [["x", "plain"], ["\uFEFFx", "\uFEFFbody"]]);
    db.close();
});
import { readFileSync } from "node:fs";
import { historyJSON } from "../src/broadcast-client";
test("Rust room wire vectors preserve distinct BOM IDs and body through TypeScript front", () => {
    const vectors = readFileSync("../../crates/wasmkeep-sdk/src/broadcast/unicode-vectors.txt", "utf8").trim().split("\n").map(s => Uint8Array.from(s.match(/../g)!, h => parseInt(h, 16)));
    let poked = 0;
    host((address, req) => {
        // The Rust room's answer asks for a poke (a cold room's pusher is idle).
        if (address === "pusher/rust") {
            poked++;
            return { status: 200, headers: [], body: new Uint8Array() };
        }
        const i = req.path.endsWith("publish") ? 0 : 2;
        assert.deepEqual(req.body, vectors[i]);
        return { status: 200, headers: [], body: vectors[i + 1] };
    });
    const c = new Client("room/rust");
    assert.deepEqual(c.publish([{ id: "x", body: "plain" }, { id: "\uFEFFx", body: "\uFEFFbody" }]), [{ e: 1n, n: 1n }, { e: 1n, n: 2n }]);
    assert.equal(poked, 1);
    const h = historyJSON(c.history({ e: 1n, n: 0n }));
    assert.deepEqual(h.messages.map(m => [m.id, m.body]), [["x", "plain"], ["\uFEFFx", "\uFEFFbody"]]);
});
