/** One directory map per app-owned actor (ADR 0040). Export `define()` as
 * an internal class, or delegate your class's init/fetch to it. The handler
 * never calls another actor. Only the map's creator may change it. */
import * as storage from "wasmkeep:actor/storage@0.2.0";
import type { Value } from "wasmkeep:actor/types@0.2.0";
import type { ActorDefinition, ActorRequest, ActorResponse } from "./index";
import { Reader, Writer, header } from "./wire";
import { PREFIX, DirectoryError, create, change, lookup, encodeMap, decodeChange, writeEntry, number, text, bytes, end, invalid, type State, type Entry } from "./directory-state";
export { Client } from "./directory-client";
export { point, DirectoryError, MAX_ENTRIES, MAX_META, type Map, type Entry, type Change, type Keep } from "./directory-state";
const integer = (val: bigint): Value => ({ tag: "integer", val });
const string = (val: string): Value => ({ tag: "text", val });
const blob = (val: Uint8Array): Value => ({ tag: "blob", val });
const bound = (n: bigint) => blob(new Writer().u64(n).finish());
function rows(sql: string): Record<string, Value>[] {
    const rows = storage.query(sql, []);
    return rows.values.map(values => Object.fromEntries(rows.columns.map((c, i) => [c, values[i]])));
}
function value<T extends Value["tag"]>(v: Value, tag: T): Extract<Value, {
    tag: T;
}> {
    if (v.tag !== tag)
        throw new Error("invalid directory state");
    return v as Extract<Value, {
        tag: T;
    }>;
}
function load(): State {
    const [m] = rows("SELECT owner,version,last_op,last_result FROM wasmkeep_directory_map WHERE id=1");
    if (!m)
        throw new DirectoryError(404, "no map");
    const entries: Entry[] = rows("SELECT lo,target,version,meta FROM wasmkeep_directory_entries ORDER BY lo").map(e => ({ lo: number(new Reader(value(e.lo, "blob").val)), hi: undefined, target: value(e.target, "text").val, version: value(e.version, "integer").val, meta: value(e.meta, "blob").val }));
    for (let i = 1; i < entries.length; i++)
        entries[i - 1].hi = entries[i].lo;
    return { owner: value(m.owner, "text").val, map: { version: value(m.version, "integer").val, entries }, lastOp: m.last_op.tag === "null" ? undefined : value(m.last_op, "text").val, lastResult: m.last_result.tag === "null" ? undefined : value(m.last_result, "blob").val };
}
function save(before: State | undefined, after: State): void {
    const oldEntries = new Map((before?.map.entries ?? []).map(e => [e.lo, e]));
    const newBounds = new Set(after.map.entries.map(e => e.lo));
    for (const e of before?.map.entries ?? [])
        if (!newBounds.has(e.lo))
            storage.exec("DELETE FROM wasmkeep_directory_entries WHERE lo=?1", [bound(e.lo)]);
    for (const e of after.map.entries) {
        if (oldEntries.get(e.lo)?.version === e.version)
            continue;
        storage.exec("INSERT INTO wasmkeep_directory_entries (lo,target,version,meta) VALUES (?1,?2,?3,?4) ON CONFLICT(lo) DO UPDATE SET target=excluded.target,version=excluded.version,meta=excluded.meta", [bound(e.lo), string(e.target), integer(e.version), blob(e.meta)]);
    }
    storage.exec("INSERT INTO wasmkeep_directory_map (id,owner,version,last_op,last_result) VALUES (1,?1,?2,?3,?4) ON CONFLICT(id) DO UPDATE SET version=excluded.version,last_op=excluded.last_op,last_result=excluded.last_result", [string(after.owner), integer(after.map.version), after.lastOp === undefined ? { tag: "null" } : string(after.lastOp), after.lastResult === undefined ? { tag: "null" } : blob(after.lastResult)]);
}
function transaction(f: () => Uint8Array): Uint8Array {
    storage.exec("BEGIN", []);
    try {
        const body = f();
        storage.execDurable("COMMIT", []);
        return body;
    }
    catch (e) {
        try {
            storage.exec("ROLLBACK", []);
        }
        catch { /* Commit may already have applied. */ }
        throw e;
    }
}
function serve(caller: string, op: string, body: Uint8Array): Uint8Array {
    const r = new Reader(body);
    switch (op) {
        case "create": {
            const target = text(r), meta = bytes(r);
            end(r);
            return transaction(() => {
                let current: State | undefined;
                try {
                    current = load();
                }
                catch (e) {
                    if (!(e instanceof DirectoryError && e.status === 404))
                        throw e;
                }
                if (current) {
                    if (current.owner !== caller)
                        throw new DirectoryError(409, "already owned", current.map.version);
                    return encodeMap(current.map);
                }
                const next = create(caller, target, meta);
                save(undefined, next);
                return encodeMap(next.map);
            });
        }
        case "split":
        case "merge": {
            const c = decodeChange(op, r);
            end(r);
            return transaction(() => { const before = load(), after = change(before, caller, c); if (after !== before)
                save(before, after); return after.lastResult ?? invalid(); });
        }
        case "transfer": {
            // The owner names its successor (ADR 0040, section 13); the
            // map's version is unchanged. Repeated: already done.
            const owner = text(r);
            end(r);
            return transaction(() => {
                const state = load();
                if (state.owner !== owner) {
                    if (state.owner !== caller)
                        throw new DirectoryError(403, "not the owner");
                    storage.exec("UPDATE wasmkeep_directory_map SET owner=?1 WHERE id=1", [string(owner)]);
                }
                return new Uint8Array();
            });
        }
        case "entries":
            end(r);
            return encodeMap(load().map);
        case "lookup": {
            const p = number(r);
            end(r);
            const { map } = load();
            return writeEntry(new Writer().u64(map.version), lookup(map, p)).finish();
        }
        case "delete": {
            const expect = number(r);
            end(r);
            const state = load();
            if (state.owner !== caller)
                throw new DirectoryError(403, "not the owner");
            if (expect !== state.map.version)
                throw new DirectoryError(409, "version conflict", state.map.version);
            storage.deleteSelfDurable();
            return new Uint8Array();
        }
        default: throw new DirectoryError(404, "not a directory path");
    }
}
/** Initializes the directory's tables (no map yet). */
export function init(): void {
    storage.exec("CREATE TABLE IF NOT EXISTS wasmkeep_directory_map (id INTEGER PRIMARY KEY CHECK(id=1), owner TEXT NOT NULL, version INTEGER NOT NULL, last_op TEXT, last_result BLOB)", []);
    storage.exec("CREATE TABLE IF NOT EXISTS wasmkeep_directory_entries (lo BLOB PRIMARY KEY CHECK(length(lo)=8), target TEXT NOT NULL, version INTEGER NOT NULL, meta BLOB NOT NULL)", []);
}
/** Trusted inside callers only, including for reads. */
export function handle(req: ActorRequest): ActorResponse {
    try {
        const caller = header(req.headers, "x-wasmkeep-caller");
        if (!caller)
            throw new DirectoryError(403, "inside callers only");
        if (req.method !== "POST")
            invalid();
        if (!req.path.startsWith(PREFIX))
            throw new DirectoryError(404, "not a directory path");
        const body = req.bytes(16 * 1024 * 1024);
        if (body === undefined)
            invalid();
        return { status: 200, body: serve(caller, req.path.slice(PREFIX.length), body) };
    }
    catch (e) {
        if (e instanceof DirectoryError)
            return { status: e.status, body: e.status === 409 ? new Writer().u64(e.version!).finish() : e.message };
        return { status: 500, body: "directory storage failure" };
    }
}
export function define(): ActorDefinition { return { init, fetch: handle }; }
