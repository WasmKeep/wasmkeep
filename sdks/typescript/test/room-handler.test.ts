import { test } from "node:test";
import assert from "node:assert/strict";
import { defineEdge } from "../src/broadcast-edge";
import * as db from "./directory-storage-stub";
import * as sockets from "./sockets-stub";
import type { ActorRequest } from "../src/request";
test("edge handler ownership, malformed payloads and positions", () => {
    db.install(":memory:");
    const edge = defineEdge();
    edge.init!();
    const req = (op: string, body: string, caller = "tenant/room/r") => {
        const request: ActorRequest = { method: "POST", path: `/wasmkeep-edge/v1/${op}`, headers: { "x-wasmkeep-caller": caller }, text: () => body, bytes: () => new TextEncoder().encode(body), json: <T>() => JSON.parse(body) as T };
        const result = edge.fetch!(request);
        assert(typeof result === "object" && !(result instanceof Uint8Array));
        return result.status;
    };
    assert.equal(req("init", '{}', "outside"), 403);
    assert.equal(req("init", JSON.stringify({ room: "tenant/room/r", lo: "0000000000000000", state: "active" })), 200);
    assert.equal(req("push", '{}', "tenant/other/r"), 403);
    for (const body of ['{', 'null', JSON.stringify({ e: 1, b: 0, first: 1, head: 1, messages: [{ a: 0, d: 42 }] }), JSON.stringify({ e: 1, b: 0, first: 1, head: 1, messages: [{ a: 0, d: "x", e: 2, b: 0 }] })])
        assert.equal(req("push", body), 400);
    for (const suffix of ['&e=1', '&e=&n=', '&e=0x1&n=2', '&e=1&n=9007199254740992'])
        assert.throws(() => edge.websocket!.open!(1n, '/?c=a&p=0000000000000000' + suffix));
    db.close();
});
import { readFileSync } from "node:fs";
test("shared push vectors through the edge handler", () => {
    db.install(":memory:");
    const e = defineEdge();
    e.init!();
    const call = (path: string, body: string) => { const r = e.fetch!({ method: "POST", path, headers: { "x-wasmkeep-caller": "tenant/room/r" }, text: () => body, bytes: () => new TextEncoder().encode(body), json: <T>() => JSON.parse(body) as T }); assert(typeof r === "object" && !(r instanceof Uint8Array)); return r.status; };
    assert.equal(call("/wasmkeep-edge/v1/init", JSON.stringify({ room: "tenant/room/r", lo: "0000000000000000", state: "active" })), 200);
    for (const line of readFileSync("../../crates/wasmkeep-sdk/src/broadcast/push-vectors.txt", "utf8").trim().split("\n").filter(s => !s.startsWith("#")))
        assert.equal(call("/wasmkeep-edge/v1/push", line.slice(2)), line[0] === "1" ? 200 : 400, line);
    db.close();
});
test("an ended unknown edge refuses and deletes itself", () => {
    db.install(":memory:");
    const e = defineEdge();
    e.init!();
    assert.throws(() => e.websocket!.open!(1n, "/?c=a&p=0000000000000000"), /unknown/);
    assert(db.deleted);
    db.close();
});
test("an edge woken with sockets announces their start, delivers its first push and answers pings", () => {
    db.install(":memory:");
    // A fresh definition is a woken instance: no head, no cursors.
    const e = defineEdge();
    e.init!();
    const call = (path: string, body: string) => { const r = e.fetch!({ method: "POST", path, headers: { "x-wasmkeep-caller": "tenant/room/r" }, text: () => body, bytes: () => new TextEncoder().encode(body), json: <T>() => JSON.parse(body) as T }); assert(typeof r === "object" && !(r instanceof Uint8Array)); return r.status; };
    assert.equal(call("/wasmkeep-edge/v1/init", JSON.stringify({ room: "tenant/room/r", lo: "0000000000000000", state: "active" })), 200);
    sockets.open.push(7n);
    try {
        e.websocket!.message!(7n, '{"t":"ping"}');
        assert.deepEqual(sockets.sent.splice(0).map(([id, s]) => [id, JSON.parse(s)]), [[7n, { t: "pong", e: 0, n: null }]]);
        assert.equal(call("/wasmkeep-edge/v1/push", JSON.stringify({ e: 1, b: 0, first: 374, head: 375, messages: [{ a: 0, d: "374" }, { a: 0, d: "375" }] })), 200);
        const frames = sockets.sent.splice(0).map(([id, s]) => [id, JSON.parse(s)]);
        assert.deepEqual(frames, [[7n, { t: "ready", e: 1, b: 0, n: 373 }], [7n, { t: "m", e: 1, b: 0, n: 374, m: [{ a: 0, d: "374" }, { a: 0, d: "375" }] }]]);
    }
    finally {
        sockets.open.splice(0);
        sockets.sent.splice(0);
        db.close();
    }
});
