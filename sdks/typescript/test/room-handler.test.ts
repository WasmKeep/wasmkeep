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
        // Header-only: the socket learns where the stream starts.
        assert.equal(call("/wasmkeep-edge/v1/push", JSON.stringify({ e: 1, b: 0, first: 376, head: 375, messages: [] })), 200);
        assert.deepEqual(sockets.sent.splice(0).map(([id, s]) => [id, JSON.parse(s)]), [[7n, { t: "ready", e: 1, b: 0, n: 375 }]]);
        assert.equal(call("/wasmkeep-edge/v1/push", JSON.stringify({ e: 1, b: 0, first: 376, head: 376, messages: [{ a: 0, d: "376" }] })), 200);
        assert.deepEqual(sockets.sent.splice(0).map(([id, s]) => [id, JSON.parse(s)]), [[7n, { t: "m", e: 1, b: 0, n: 376, m: [{ a: 0, d: "376" }] }]]);
        // A second woken instance whose first push carries messages: they
        // go out, with no announcement.
        const woken = defineEdge();
        const again = (body: string) => { const r = woken.fetch!({ method: "POST", path: "/wasmkeep-edge/v1/push", headers: { "x-wasmkeep-caller": "tenant/room/r" }, text: () => body, bytes: () => new TextEncoder().encode(body), json: <T>() => JSON.parse(body) as T }); assert(typeof r === "object" && !(r instanceof Uint8Array)); return r.status; };
        assert.equal(again(JSON.stringify({ e: 1, b: 0, first: 374, head: 375, messages: [{ a: 0, d: "374" }, { a: 0, d: "375" }] })), 200);
        assert.deepEqual(sockets.sent.splice(0).map(([id, s]) => [id, JSON.parse(s)]), [[7n, { t: "m", e: 1, b: 0, n: 374, m: [{ a: 0, d: "374" }, { a: 0, d: "375" }] }]]);
    }
    finally {
        sockets.open.splice(0);
        sockets.sent.splice(0);
        db.close();
    }
});
test("an edge woken with sockets and a stored head still announces where its first push starts", () => {
    db.install(":memory:");
    const fetch = (e: ReturnType<typeof defineEdge>, path: string, body: string) => {
        const r = e.fetch!({ method: "POST", path, headers: { "x-wasmkeep-caller": "tenant/pusher/r" }, text: () => body, bytes: () => new TextEncoder().encode(body), json: <T>() => JSON.parse(body) as T });
        assert(typeof r === "object" && !(r instanceof Uint8Array));
        return r.status;
    };
    const config = { room: "tenant/pusher/r", lo: "0000000000000000", state: "active", e: 1, n: 3 };
    const frames = () => sockets.sent.splice(0).map(([id, s]) => [id, JSON.parse(s)]);
    sockets.open.push(7n);
    try {
        // A replayed init on an edge that kept its row (and a socket) through
        // a hibernation: the stored head (1, 3) is the edge's, but the socket
        // is told where the first push starts, not silently placed there.
        const e = defineEdge();
        e.init!();
        assert.equal(fetch(e, "/wasmkeep-edge/v1/init", JSON.stringify(config)), 200);
        const woken = defineEdge();
        assert.equal(fetch(woken, "/wasmkeep-edge/v1/init", JSON.stringify({ ...config, n: 9 })), 200);
        assert.deepEqual(frames(), []);
        assert.equal(fetch(woken, "/wasmkeep-edge/v1/push", JSON.stringify({ e: 1, b: 0, first: 6, head: 5, messages: [] })), 200);
        assert.deepEqual(frames(), [[7n, { t: "ready", e: 1, b: 0, n: 3 }]]);
        assert.equal(fetch(woken, "/wasmkeep-edge/v1/push", JSON.stringify({ e: 1, b: 0, first: 4, head: 5, messages: [{ a: 0, d: "4" }, { a: 0, d: "5" }] })), 200);
        assert.deepEqual(frames(), [[7n, { t: "m", e: 1, b: 0, n: 4, m: [{ a: 0, d: "4" }, { a: 0, d: "5" }] }]]);
        // A range on a woken edge stores a head too; a client that joined
        // before it (told n: null) gets the first push live, no behind.
        const ranged = defineEdge();
        ranged.websocket!.open!(8n, "/?c=c&p=0000000000000001&e=1&n=2");
        sockets.open.push(8n);
        assert.deepEqual(frames(), [[8n, { t: "ready", e: 0, n: null }]]);
        assert.equal(fetch(ranged, "/wasmkeep-edge/v1/range", JSON.stringify({ ...config, n: 4 })), 200);
        // The range closed the kept socket, whose point it does not know.
        sockets.open.splice(0, 1);
        assert.equal(fetch(ranged, "/wasmkeep-edge/v1/push", JSON.stringify({ e: 1, b: 0, first: 5, head: 5, messages: [{ a: 0, d: "5" }] })), 200);
        assert.deepEqual(frames(), [[8n, { t: "m", e: 1, b: 0, n: 5, m: [{ a: 0, d: "5" }] }]]);
    }
    finally {
        sockets.open.splice(0);
        sockets.sent.splice(0);
        db.close();
    }
});
