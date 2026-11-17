import { test } from "node:test";
import assert from "node:assert/strict";
import { Core } from "../src/broadcast-core";
import * as db from "./directory-storage-stub";
function room(confirm = false) {
    return new Core(db, { setAlarm(n: bigint) {
            alarm = n;
        } }, confirm);
}
let alarm = 0n;
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
test("confirm flush before push, retry after 50ms", () => {
    db.install(":memory:");
    let fail = true, calls = 0;
    const r = new Core({ ...db, flush() {
            calls++;
            if (fail)
                throw { kind: "internal", message: "not durable: still uploading" };
        } }, { setAlarm(n: bigint) {
            alarm = n;
        } }, true);
    r.init();
    r.publish([{ body: "a" }], 0n);
    assert.equal(r.beforePush(20n), false);
    assert.equal(alarm, 70n);
    fail = false;
    assert.equal(r.beforePush(70n), true);
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
    assert(r.history({ e: 1n, n: 1n }).reset);
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
