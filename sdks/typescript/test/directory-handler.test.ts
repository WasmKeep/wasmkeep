import { test } from "node:test";
import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { install as host } from "./host-stub";
import * as sqlite from "./directory-storage-stub";
import { define, Client, DirectoryError } from "../src/directory";
import { Writer, toBytes } from "../src/wire";
import type { ActorRequest } from "../src/request";
test("directory class/client on SQLite: transaction isolation, rollback, replay, owner and delete", () => {
    const dir = mkdtempSync(tmpdir() + "/directory-test-");
    const path = dir + "/state.db";
    sqlite.install(path);
    const actor = define();
    actor.init!();
    let caller = "tenant/app/owner";
    const request = (op: string, body: Uint8Array, who = caller) => {
        const req: ActorRequest = { method: "POST", path: "/wasmkeep-directory/v1/" + op, headers: who ? { "x-wasmkeep-caller": who } : {}, bytes: () => body, text: () => "", json: <T>() => ({}) as T };
        const response = actor.fetch!(req);
        assert(typeof response === "object" && !(response instanceof Uint8Array));
        return { status: response.status ?? 200, headers: [], body: typeof response.body === "string" ? toBytes(response.body) : response.body ?? new Uint8Array() };
    };
    host((address, req) => { assert.equal(address, "directory/map"); return request(req.path.split("/").at(-1)!, req.body); });
    try {
        const c = new Client("directory/map");
        assert.throws(() => c.entries(), e => e instanceof DirectoryError && e.status === 404);
        assert.equal(request("entries", new Uint8Array(), "").status, 403);
        const original = c.create("edge/a", new Uint8Array([1]));
        caller = "tenant/app/other";
        assert.throws(() => c.create("edge/a"), e => e instanceof DirectoryError && e.status === 409);
        assert.deepEqual(c.entries(), original);
        caller = "tenant/app/owner";
        assert.deepEqual(c.create("edge/ignored"), original);
        const split = { kind: "split" as const, lo: 0n, at: 1n << 63n, target: "edge/b", meta: new Uint8Array([2]), expect: 1n, op: "split" };
        sqlite.onCommit(() => {
            const reader = new DatabaseSync(path);
            try {
                assert.equal(reader.prepare("SELECT version FROM wasmkeep_directory_map").all()[0].version, 1);
                assert.equal(reader.prepare("SELECT lo FROM wasmkeep_directory_entries").all().length, 1);
            }
            finally {
                reader.close();
            }
        });
        const divided = c.change(split);
        assert.equal(divided.version, 2n);
        assert.equal(c.lookup((1n << 64n) - 1n).entry.target, "edge/b");
        assert.deepEqual(c.change(split), divided);
        caller = "tenant/app/other";
        assert.throws(() => c.change(split), e => e instanceof DirectoryError && e.status === 403);
        caller = "tenant/app/owner";
        assert.throws(() => c.change({ ...split, op: "stale" }), e => e instanceof DirectoryError && e.version === 2n);
        const merge = { kind: "merge" as const, lo: 1n << 63n, keep: "right" as const, expect: 2n, op: "merge" };
        sqlite.failNextCommit();
        assert.throws(() => c.change(merge));
        assert.deepEqual(c.entries(), divided);
        const merged = c.change(merge);
        assert.equal(merged.entries.length, 1);
        assert.equal(merged.entries[0].target, "edge/b");
        // Reopen storage: replay lives in SQLite, not an instance variable.
        sqlite.close();
        sqlite.install(path);
        assert.deepEqual(c.change(merge), merged);
        assert.equal(request("split", new Uint8Array([0])).status, 400);
        assert.equal(request("merge", new Writer().u64(1n).u64(2n).u64(3n).bytes(toBytes("invalid")).finish()).status, 400);
        assert.throws(() => c.delete(2n), e => e instanceof DirectoryError && e.version === 3n);
        c.delete(3n);
        assert(sqlite.deleted);
        assert.throws(() => c.lookup(0n), e => e instanceof DirectoryError && e.status === 404);
    }
    finally {
        sqlite.close();
        rmSync(dir, { recursive: true, force: true });
    }
});
