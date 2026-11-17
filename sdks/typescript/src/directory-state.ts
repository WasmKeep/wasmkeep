// Pure directory state and v1 framing, shared semantically with Rust.
import { Reader, Writer, toBytes, toU64 } from "./wire";
export const MAX_ENTRIES = 4096, MAX_META = 1024;
export const PREFIX = "/wasmkeep-directory/v1/";
/** A half-open range; absent hi means 2^64. All integers stay exact. */
export interface Entry {
    lo: bigint;
    hi?: bigint;
    target: string;
    version: bigint;
    meta: Uint8Array;
}
export interface Map {
    version: bigint;
    entries: Entry[];
}
export type Keep = "left" | "right";
export type Change = ({
    kind: "split";
    lo: bigint;
    at: bigint;
    target: string;
    meta: Uint8Array;
} | {
    kind: "merge";
    lo: bigint;
    keep: Keep;
}) & {
    expect: bigint;
    op: string;
};
export interface State {
    owner: string;
    map: Map;
    lastOp?: string;
    lastResult?: Uint8Array;
}
export class DirectoryError extends Error {
    constructor(readonly status: number, message: string, readonly version?: bigint) { super(message); this.name = "DirectoryError"; }
}
export function invalid(): never { throw new DirectoryError(400, "invalid directory request or reply"); }
export function targetMeta(target: string, meta: Uint8Array): void {
    const slash = target.indexOf("/");
    if (slash < 1 || slash === target.length - 1 || toBytes(target).length > 1024 || /[\x00-\x1f\x7f-\x9f]/.test(target) || meta.length > MAX_META)
        invalid();
}
export function point(key: string | Uint8Array): bigint {
    let h = 0xcbf29ce484222325n;
    for (const b of toBytes(key))
        h = BigInt.asUintN(64, (h ^ BigInt(b)) * 0x100000001b3n);
    h ^= h >> 33n;
    h = BigInt.asUintN(64, h * 0xff51afd7ed558ccdn);
    h ^= h >> 33n;
    h = BigInt.asUintN(64, h * 0xc4ceb9fe1a85ec53n);
    return h ^ (h >> 33n);
}
export function create(owner: string, target: string, meta: Uint8Array): State {
    targetMeta(target, meta);
    return { owner, map: { version: 1n, entries: [{ lo: 0n, hi: undefined, target, meta: meta.slice(), version: 1n }] } };
}
export function lookup(map: Map, p: bigint): Entry {
    toU64(p);
    const e = map.entries.find(e => e.lo <= p && (e.hi === undefined || p < e.hi));
    if (!e)
        return invalid();
    return e;
}
export function change(state: State, caller: string, c: Change): State {
    if (caller !== state.owner)
        throw new DirectoryError(403, "not the owner");
    if (!c.op || toBytes(c.op).length > 1024)
        invalid();
    if (state.lastOp === c.op)
        return state;
    if (c.expect !== state.map.version)
        throw new DirectoryError(409, "version conflict", state.map.version);
    const version = state.map.version + 1n;
    if (version > 0x7fffffffffffffffn)
        invalid();
    const entries = state.map.entries.map(e => ({ ...e, meta: e.meta.slice() }));
    const i = entries.findIndex(e => e.lo === c.lo);
    if (i < 0)
        invalid();
    if (c.kind === "split") {
        toU64(c.at);
        targetMeta(c.target, c.meta);
        if (entries.length >= MAX_ENTRIES || c.at <= c.lo || (entries[i].hi !== undefined && c.at >= entries[i].hi!))
            invalid();
        const hi = entries[i].hi;
        entries[i].hi = c.at;
        entries[i].version = version;
        entries.splice(i + 1, 0, { lo: c.at, hi, target: c.target, meta: c.meta.slice(), version });
    }
    else {
        if (i === 0 || (c.keep !== "left" && c.keep !== "right"))
            invalid();
        const [right] = entries.splice(i, 1);
        entries[i - 1].hi = right.hi;
        entries[i - 1].version = version;
        if (c.keep === "right") {
            entries[i - 1].target = right.target;
            entries[i - 1].meta = right.meta;
        }
    }
    const map = { version, entries };
    return { owner: state.owner, map, lastOp: c.op, lastResult: encodeMap(map) };
}
export function writeEntry(w: Writer, e: Entry): Writer {
    return w.u64(e.lo).u64(e.hi === undefined ? 0 : 1).u64(e.hi ?? 0n).bytes(toBytes(e.target)).u64(e.version).bytes(e.meta);
}
export function number(r: Reader): bigint { const n = r.u64(); if (n === undefined)
    return invalid(); return n; }
export function bytes(r: Reader): Uint8Array { const b = r.bytes(); if (b === undefined)
    return invalid(); return b; }
export function text(r: Reader): string {
    try {
        return new TextDecoder("utf-8", { fatal: true, ignoreBOM: true }).decode(bytes(r));
    }
    catch {
        return invalid();
    }
}
export function end(r: Reader): void { if (!r.isEmpty())
    invalid(); }
export function readEntry(r: Reader): Entry {
    const lo = number(r), has = number(r), upper = number(r);
    if (!((has === 0n && upper === 0n) || (has === 1n && upper > lo)))
        invalid();
    const target = text(r), version = number(r), meta = bytes(r);
    targetMeta(target, meta);
    return { lo, hi: has === 1n ? upper : undefined, target, version, meta };
}
export function encodeMap(map: Map): Uint8Array {
    const w = new Writer().u64(map.version).u64(map.entries.length);
    for (const e of map.entries)
        writeEntry(w, e);
    return w.finish();
}
export function decodeMap(body: Uint8Array): Map {
    const r = new Reader(body), version = number(r), count = number(r);
    if (count === 0n || count > BigInt(MAX_ENTRIES) || version === 0n || version > 0x7fffffffffffffffn)
        invalid();
    const entries: Entry[] = [];
    for (let i = 0n; i < count; i++)
        entries.push(readEntry(r));
    end(r);
    if (entries[0].lo !== 0n || entries.at(-1)!.hi !== undefined || entries.some((e, i) => e.version === 0n || e.version > version || (i > 0 && entries[i - 1].hi !== e.lo)))
        invalid();
    return { version, entries };
}
export function encodeChange(c: Change): Uint8Array {
    const w = new Writer().u64(c.lo);
    if (c.kind === "split")
        w.u64(c.at).bytes(toBytes(c.target)).bytes(c.meta);
    else {
        if (c.keep !== "left" && c.keep !== "right")
            invalid();
        w.u64(c.keep === "right" ? 1 : 0);
    }
    return w.u64(c.expect).bytes(toBytes(c.op)).finish();
}
export function decodeChange(kind: "split" | "merge", r: Reader): Change {
    const lo = number(r);
    if (kind === "split")
        return { kind, lo, at: number(r), target: text(r), meta: bytes(r), expect: number(r), op: text(r) };
    const keep = number(r);
    if (keep !== 0n && keep !== 1n)
        invalid();
    return { kind, lo, keep: keep === 0n ? "left" : "right", expect: number(r), op: text(r) };
}
