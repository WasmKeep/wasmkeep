// Real SQLite behind the WIT storage interface. Tests can fail a commit or
// inspect a second connection just before COMMIT, while changes are private.
import { DatabaseSync } from "node:sqlite";
import type { Value, Rows } from "wasmkeep:actor/types@0.2.0";
let db: DatabaseSync;
export let commits = 0;
export let deleted = false;
let beforeCommit: (() => void) | undefined;
let failCommit = false;
const named = new Map<string, DatabaseSync>();
export function install(path: string): void { db = new DatabaseSync(path); named.clear(); commits = 0; deleted = false; beforeCommit = undefined; failCommit = false; }
/** Tests of several actors: each name its own in-memory database. */
export function use(name: string): void { let d = named.get(name); if (!d) { d = new DatabaseSync(":memory:"); named.set(name, d); } db = d; }
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
// node:sqlite before Node 26 reads a TEXT value only up to its first U+0000
// (it is stored whole: binding is right). The host (rusqlite) reads all of
// it, so where node truncates, text columns are read as bytes and decoded.
const truncatesAtNul = (() => {
    const probe = new DatabaseSync(":memory:");
    const got = probe.prepare("SELECT ?1 AS v").all("\0x")[0].v;
    probe.close();
    return got !== "\0x";
})();
const utf8 = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
const ident = (name: string) => `"${name.replaceAll('"', '""')}"`;
/** The rows with TEXT read whole, or undefined when the query cannot be wrapped (not a plain SELECT, or repeated column names). */
function wholeText(sql: string, params: Value[], columns: string[]): Value[][] | undefined {
    if (new Set(columns).size !== columns.length)
        return undefined;
    const cells = columns.map((c, i) => `typeof(q.${ident(c)}) AS t${i}, CASE typeof(q.${ident(c)}) WHEN 'text' THEN CAST(q.${ident(c)} AS BLOB) ELSE q.${ident(c)} END AS v${i}`);
    let stmt;
    try {
        stmt = db.prepare(`SELECT ${cells.join(", ")} FROM (${sql}) AS q`);
    }
    catch {
        return undefined;
    }
    stmt.setReadBigInts(true);
    const rows = stmt.all(...params.map(param)) as Record<string, unknown>[];
    return rows.map(row => columns.map((_, i): Value => row[`t${i}`] === "text" ? { tag: "text", val: utf8.decode(row[`v${i}`] as Uint8Array) } : result(row[`v${i}`])));
}
export function query(sql: string, params: Value[]): Rows {
    const stmt = db.prepare(sql);
    stmt.setReadBigInts(true);
    const columns = stmt.columns().map(c => c.name);
    const whole = truncatesAtNul && columns.length > 0 ? wholeText(sql, params, columns) : undefined;
    if (whole)
        return { columns, values: whole };
    const all = stmt.all(...params.map(param));
    return { columns, values: all.map(row => columns.map(c => result(row[c]))) };
}
export function deleteSelfDurable(): void { deleted = true; db.exec("DELETE FROM wasmkeep_directory_entries; DELETE FROM wasmkeep_directory_map"); }
export function flush(): void {}
export function deleteSelf(): void { deleted = true; }
