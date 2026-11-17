import { call } from "./call";
import { Reader, Writer, toBytes } from "./wire";
import { PREFIX, DirectoryError, decodeMap, encodeChange, readEntry, number, end, targetMeta, type Map, type Entry, type Change } from "./directory-state";
/** Reaches one directory actor. No lookup cache. Retry the latest change
 * with the same op id before submitting another operation. */
export class Client {
    constructor(readonly address: string) { }
    private call(op: string, body: Uint8Array = new Uint8Array()): Uint8Array {
        const result = call(this.address, "POST", PREFIX + op, [], body);
        if ("error" in result)
            throw new Error(result.error);
        const { status, body: reply } = result.reply;
        if (status !== 200)
            throw new DirectoryError(status, "directory refused request", status === 409 ? number(new Reader(reply)) : undefined);
        return reply;
    }
    create(target: string, meta: Uint8Array = new Uint8Array()): Map { targetMeta(target, meta); return decodeMap(this.call("create", new Writer().bytes(toBytes(target)).bytes(meta).finish())); }
    entries(): Map { return decodeMap(this.call("entries")); }
    lookup(point: bigint): {
        entry: Entry;
        version: bigint;
    } {
        const r = new Reader(this.call("lookup", new Writer().u64(point).finish()));
        const version = number(r), entry = readEntry(r);
        end(r);
        return { entry, version };
    }
    change(c: Change): Map { return decodeMap(this.call(c.kind, encodeChange(c))); }
    delete(expect: bigint): void { this.call("delete", new Writer().u64(expect).finish()); }
}
