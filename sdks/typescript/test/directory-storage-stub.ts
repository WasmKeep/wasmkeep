// Real SQLite behind the WIT storage interface. Tests can fail a commit or
// inspect a second connection just before COMMIT, while changes are private.
import { DatabaseSync } from "node:sqlite";
import type { Value, Rows } from "wasmkeep:actor/types@0.2.0";
let db: DatabaseSync;
export let commits = 0;
export let deleted = false;
let beforeCommit: (() => void) | undefined;
let failCommit = false;
export function install(path: string): void { db = new DatabaseSync(path); commits = 0; deleted = false; beforeCommit = undefined; failCommit = false; }
export function close(): void { db.close(); }
export function onCommit(fn: () => void): void { beforeCommit = fn; }
export function failNextCommit(): void { failCommit = true; }
function param(v: Value): unknown { return v.tag === "null" ? null : v.val; }
function result(v: unknown): Value {
    if (v === null)
        return { tag: "null" };
    if (typeof v === "bigint")
        return { tag: "integer", val: v };
    if (typeof v === "number")
        return { tag: "real", val: v };
    if (typeof v === "string")
        return { tag: "text", val: v };
    return { tag: "blob", val: v as Uint8Array };
}
export function exec(sql: string, params: Value[]): bigint { return BigInt(db.prepare(sql).run(...params.map(param)).changes); }
export function execDurable(sql: string, params: Value[]): bigint {
    const hook = beforeCommit;
    beforeCommit = undefined;
    hook?.();
    if (failCommit) {
        failCommit = false;
        throw new Error("injected commit failure");
    }
    const n = exec(sql, params);
    commits++;
    return n;
}
export function query(sql: string, params: Value[]): Rows {
    const stmt = db.prepare(sql);
    stmt.setReadBigInts(true);
    const all = stmt.all(...params.map(param));
    const columns = stmt.columns().map(c => c.name);
    return { columns, values: all.map(row => columns.map(c => result(row[c]))) };
}
export function deleteSelfDurable(): void { deleted = true; db.exec("DELETE FROM wasmkeep_directory_entries; DELETE FROM wasmkeep_directory_map"); }
export function flush(): void {}
export function deleteSelf(): void { deleted = true; }
