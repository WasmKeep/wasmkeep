/** UTF-8 for the room's classes (ADR 0040), which hold bulk text: a full
 * window, a 512 KiB push, a 256 KiB history page. QuickJS has no native
 * `TextEncoder`: the SDK's (./text.ts) runs in the interpreter at about
 * 7 MB/s, so one 512 KiB push cost 70 ms to encode and 100 ms to decode,
 * over the default 50 ms budget on its own. SQLite converts natively (host
 * time, outside the guest's CPU budget): a `CAST` round trip costs under
 * 2 ms for the same text. Small strings, and engines with native codecs
 * (StarlingMonkey, node), keep the plain path. */
import * as storage from "wasmkeep:actor/storage@0.2.0";
import { Utf8Encoder } from "./text";
const SMALL = 1024;
let forced: boolean | undefined;
/** Tests: take (or never take) the SQLite path whatever the engine. */
export function useSqlite(on: boolean | undefined) { forced = on; }
const slow = () => forced ?? (globalThis as Record<string, unknown>).TextEncoder === Utf8Encoder;
// Lone surrogates have no UTF-8 form: never lowered to the host.
const wellFormed = (s: string) => typeof (s as { isWellFormed?: () => boolean }).isWellFormed === "function" ? (s as unknown as { isWellFormed(): boolean }).isWellFormed() : !/[\ud800-\udbff](?![\udc00-\udfff])|(?:^|[^\ud800-\udbff])[\udc00-\udfff]/.test(s);
const ascii = (s: string) => !/[^\x00-\x7f]/.test(s);
function sql(statement: string, value: { tag: "text"; val: string } | { tag: "blob"; val: Uint8Array }) {
    try {
        return storage.query(statement, [value]).values[0]?.[0];
    }
    catch {
        return undefined; // a refused statement (a deleted actor, say): the plain path.
    }
}
/** The UTF-8 bytes of `s`, as `TextEncoder` encodes it. */
export function encode(s: string): Uint8Array {
    if (s.length >= SMALL && slow() && wellFormed(s)) {
        const v = sql("SELECT CAST(?1 AS BLOB)", { tag: "text", val: s });
        if (v?.tag === "blob")
            return v.val;
    }
    return new TextEncoder().encode(s);
}
/** UTF-8 byte length of `s`, as `TextEncoder` counts it. */
export function byteLength(s: string): number {
    if (!slow())
        return new TextEncoder().encode(s).length;
    if (ascii(s))
        return s.length;
    if (s.length >= 64 && wellFormed(s)) {
        const v = sql("SELECT length(CAST(?1 AS BLOB))", { tag: "text", val: s });
        if (v?.tag === "integer")
            return Number(v.val);
    }
    return new TextEncoder().encode(s).length;
}
/** Whether `s` is at most `max` bytes of UTF-8 (at most 3 per UTF-16 unit). */
export function fits(s: string, max: number): boolean {
    return s.length * 3 <= max || (s.length <= max && byteLength(s) <= max);
}
/** `bytes` as text: `fatal` throws on invalid UTF-8 as `TextDecoder` does;
 * otherwise each invalid sequence becomes U+FFFD. `stripBOM` drops a
 * leading BOM (the `TextDecoder` default). */
export function decode(bytes: Uint8Array, options: { fatal?: boolean; stripBOM?: boolean } = {}): string {
    if (bytes.length >= SMALL && slow()) {
        const v = sql("SELECT CAST(?1 AS TEXT)", { tag: "blob", val: bytes });
        // The host replaces invalid sequences: only a text without U+FFFD
        // is known to be exact when invalid input must throw.
        if (v?.tag === "text" && !(options.fatal && v.val.includes("�")))
            return options.stripBOM && v.val.charCodeAt(0) === 0xfeff ? v.val.slice(1) : v.val;
    }
    return new TextDecoder("utf-8", { fatal: options.fatal === true, ignoreBOM: !options.stripBOM }).decode(bytes);
}
