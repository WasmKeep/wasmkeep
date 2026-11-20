import { test } from "node:test";
import assert from "node:assert/strict";
import { Core, errorMessage } from "../src/broadcast-core";
import * as db from "./directory-storage-stub";
function room(confirm = false) {
    return new Core(db, confirm);
}
test("room numbering, crash void, clean wake and deduplication", () => {
    db.install(":memory:");
    let r = room();
    r.init();
    assert.deepEqual(r.publish([{ body: "a", id: "id" }], 100n), [{ e: 1n, n: 1n }]);
    assert.equal(r.publish([{ body: "retry", id: "id" }], 101n)[0].n, 1n);
    r.publish([{ body: "lost" }], 102n);
    db.exec("DELETE FROM wasmkeep_room_history WHERE n=2", []);
    r = room();
    assert.deepEqual(r.history({ e: 1n, n: 2n }).lost, { e: 2n, base: 1n });
    assert.deepEqual(r.publish([{ body: "replacement" }], 103n)[0], { e: 2n, n: 2n });
    r = room();
    assert.deepEqual(r.publish([{ body: "clean" }], 104n)[0], { e: 3n, n: 3n });
    db.close();
});
test("window, reset, gap fetch and last", () => {
    db.install(":memory:");
    const r = room();
    r.init();
    for (let n = 0; n < 2002; n++)
        r.publish([{ body: "x" }], 0n);
    assert.equal(r.history().messages.length, 0);
    const h = r.history({ e: 1n, n: 1n }, 2);
    assert.deepEqual(h.reset, { e: 1n, n: 3n });
    assert.equal(h.messages.length, 2);
    assert.equal(h.more, true);
    assert.equal(r.history({ e: 1n, n: 2001n }).messages[0].position.n, 2002n);
    assert.equal(r.history(undefined, 100, 2).messages.length, 2);
    db.close();
});
test("confirm: a pull flushes first and refuses until the upload is in", () => {
    db.install(":memory:");
    let fail = true, calls = 0;
    const r = new Core({ ...db, flush() {
            calls++;
            if (fail)
                throw { kind: "internal", message: "not durable: still uploading" };
        } }, true);
    r.init();
    r.publish([{ body: "a" }], 0n);
    assert.throws(() => r.pull({ e: 1n, n: 0n }, 1 << 20, false, 20), (e: unknown) => errorMessage(e).includes("not durable"));
    fail = false;
    assert.equal(r.pull({ e: 1n, n: 0n }, 1 << 20, false, 70).pull.messages.length, 1);
    assert.equal(calls, 2);
    db.close();
});
import { readFileSync } from "node:fs";
test("shared history vectors and epoch retention", () => {
    for (const line of readFileSync("../../crates/wasmkeep-sdk/src/broadcast/history-vectors.txt", "utf8").trim().split("\n").filter(s => !s.startsWith("#"))) {
        const v = line.split(" ").map(BigInt);
        db.install(":memory:");
        let r = room();
        r.init();
        for (let i = 0; i < 5; i++)
            r.publish([{ body: "a" }], 0n);
        db.exec("DELETE FROM wasmkeep_room_history WHERE n>4", []);
        r = room();
        r.history();
        db.exec("DELETE FROM wasmkeep_room_history WHERE n>2", []);
        r = room();
        r.publish([{ body: "new" }], 0n);
        const h = r.history({ e: v[0], n: v[1] });
        assert.deepEqual(h.lost ?? { e: 0n, base: 0n }, { e: v[2], base: v[3] });
        assert.equal(h.messages[0]?.position.n ?? 0n, v[4]);
        assert.equal(BigInt(Number(h.more)), v[5]);
        db.close();
    }
    db.install(":memory:");
    let r = room();
    r.init();
    r.publish([{ body: "a" }], 0n);
    for (let i = 0; i < 65; i++) {
        r = room();
        r.history();
    }
    assert(r.history({ e: 1n, n: 0n }).reset);
    assert.equal(db.query("SELECT * FROM wasmkeep_room_epochs", []).values.length, 64);
    db.close();
});
test("UTF-8 byte window, bounded history and invalid batch atomicity", () => {
    db.install(":memory:");
    const r = room();
    r.init();
    const body = "é".repeat(32768);
    for (let i = 0; i < 129; i++)
        r.publish([{ body }], 0n);
    const h = r.history(undefined, 2000, 2000);
    assert.equal(h.messages[0].position.n, 2n);
    assert.equal(h.messages.length, 3);
    assert.equal(h.more, true);
    assert.throws(() => r.publish([{ body: "valid" }, { body: body + "x" }], 0n));
    assert.equal(r.history().head.n, 129n);
    db.close();
});
import { Receiver, historyJSON } from "../src/room-client";
test("four retained 64-KiB messages recover across pages after 64 cold wakes", async () => {
    db.install(":memory:");
    let r = room();
    r.init();
    r.publish(Array.from({ length: 4 }, () => ({ body: "x".repeat(65536) })), 0n);
    for (let i = 0; i < 64; i++) {
        r = room();
        r.wake();
    }
    const first = r.history({ e: 1n, n: 0n });
    assert.equal(first.messages.length, 3);
    assert(first.more);
    assert(first.reset);
    const seen: number[] = [];
    const receiver = new Receiver({ history: async (p) => historyJSON(r.history(p && { e: BigInt(p.e), n: BigInt(p.n) })), onMessage: p => { seen.push(p.n); } }, { e: 1, n: 0 });
    await receiver.receive({ t: "behind", e: 65, n: 4 });
    assert.deepEqual(seen, [1, 2, 3, 4]);
    assert.deepEqual(receiver.last, { e: 65, n: 4 });
    db.close();
});
