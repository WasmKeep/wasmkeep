import { Reader, Writer } from "./wire";
import { call } from "./call";
import type { Position, Publish, History, Message } from "./broadcast-core";
import { historyJSON } from "./room-history";
const PREFIX = "/wasmkeep-room/v1/", enc = new TextEncoder(), dec = new TextDecoder("utf-8", { fatal: true, ignoreBOM: true });
function number(r: Reader): bigint {
    const n = r.u64();
    if (n === undefined)
        throw Error("invalid room protocol");
    return n;
}
function flag(r: Reader): boolean {
    const n = number(r);
    if (n > 1n)
        throw Error("invalid room flag");
    return n === 1n;
}
function count(r: Reader, max: number): number {
    const n = number(r);
    if (n > BigInt(max))
        throw Error("invalid room count");
    return Number(n);
}
function text(r: Reader): string {
    const b = r.bytes();
    if (b === undefined)
        throw Error("invalid room text");
    return dec.decode(b);
}
function end(r: Reader) {
    if (!r.isEmpty())
        throw Error("invalid room trailing bytes");
}
function position(r: Reader): Position {
    return { e: number(r), n: number(r) };
}
function put(w: Writer, p: Position) {
    w.u64(p.e).u64(p.n);
}
/** The pusher of a room address: `room/lobby` has `pusher/lobby` (the class
 * swapped, the name kept). Pass the pusher explicitly for another layout. */
export function pusherOf(room: string): string {
    const slash = room.indexOf("/");
    return `pusher/${slash < 0 ? room : room.slice(slash + 1)}`;
}
const POKE = "/wasmkeep-pusher/v1/poke";
/** What a room without `history-json` answers it (TypeScript, Rust). */
const OLD = ["invalid room operation", "invalid room protocol"].map(s => enc.encode(s));
/** Front helper; no calls from an edge or relay. */
export class Client {
    readonly pusher: string;
    constructor(readonly address: string, pusher?: string) {
        this.pusher = pusher ?? pusherOf(address);
    }
    private call(op: string, body: Uint8Array): Reader {
        const result = call(this.address, "POST", PREFIX + op, [], body);
        if ("error" in result)
            throw Error(result.error);
        if (result.reply.status !== 200)
            throw Error(`room answered ${result.reply.status}`);
        return new Reader(result.reply.body);
    }
    publish(messages: Publish[]): Position[] {
        const w = new Writer().u64(messages.length);
        for (const m of messages) {
            w.u64(m.id === undefined ? 0 : 1);
            if (m.id !== undefined)
                w.bytes(enc.encode(m.id));
            w.bytes(enc.encode(m.body));
        }
        const r = this.call("publish", w.finish()), n = count(r, 64), out: Position[] = [];
        for (let i = 0; i < n; i++)
            out.push(position(r));
        // A 0.6.0 room answers no poke field.
        const poke = r.isEmpty() ? false : flag(r);
        end(r);
        // ADR 0040, section 13: the pusher went idle and asks for a poke. A
        // failed poke is ignored: the room keeps asking until the pusher pulls.
        // Tried up to three times: the pusher's idle alarm (at most 60 s)
        // covers a poke that still fails.
        for (let attempt = 0; poke && attempt < 3; attempt++) {
            const r = call(this.pusher, "POST", POKE, [], new Writer().u64(1).finish());
            if (!("error" in r) && r.reply.status === 200)
                break;
        }
        return out;
    }
    history(after?: Position, limit = 2000, last?: number): History {
        const w = new Writer().u64(after ? 1 : 0);
        if (after)
            put(w, after);
        w.u64(limit).u64(last === undefined ? 0 : 1);
        if (last !== undefined)
            w.u64(last);
        const r = this.call("history", w.finish()), head = position(r), lost = flag(r) ? { e: number(r), base: number(r) } : undefined, reset = flag(r) ? position(r) : undefined, more = flag(r), n = count(r, 2000), messages: Message[] = [];
        for (let i = 0; i < n; i++)
            messages.push({ position: position(r), at: number(r), id: flag(r) ? text(r) : undefined, body: text(r) });
        end(r);
        return { head, lost, reset, more, messages };
    }
    /** The browser's history page, as JSON bytes to answer unread: the
     * room renders `JSON.stringify(historyJSON(history(after, limit)))`
     * itself, at most 256 rows and about 64 KiB of bodies (at least one
     * row, `more` when cut), so this front's cost does not grow with the
     * page. A room of an earlier release has no such operation: then the
     * page is converted here, as before. */
    historyJSON(after?: Position, limit = 256): Uint8Array {
        const w = new Writer().u64(after ? 1 : 0);
        if (after)
            put(w, after);
        w.u64(limit);
        const result = call(this.address, "POST", PREFIX + "history-json", [], w.finish());
        if ("error" in result)
            throw Error(result.error);
        const { status, body } = result.reply;
        if (status === 200)
            return body;
        if (status === 400 && OLD.some(o => o.length === body.length && o.every((b, i) => b === body[i])))
            return enc.encode(JSON.stringify(historyJSON(this.history(after, Math.min(limit, 256)))));
        throw Error(`room answered ${status}`);
    }
}
export { historyJSON } from "./room-history";
import * as calls from 'wasmkeep:actor/calls@0.2.0';
import * as actors from 'wasmkeep:actor/actors@0.2.0';
import { point, readEntry, number as directoryNumber, end as directoryEnd } from './directory-state';
import { handOff } from './request';
/** Authorize first. An empty room is created on its alarm, so a first join
 * can be unavailable until that alarm runs; the reconnecting client retries. */
export function join(room: string, directory: string, client: string, after?: Position, pusher = pusherOf(room)) {
    const p = point(client), lookup = { method: 'POST', path: '/wasmkeep-directory/v1/lookup', headers: [] as [
            string,
            string
        ][], body: new Writer().u64(p).finish() };
    // The poke goes to the pusher (section 13), which creates the group.
    const results = calls.callMany([[directory, lookup], [pusher, { method: 'POST', path: POKE, headers: [], body: new Uint8Array() }]]);
    const first = results[0];
    if (first.tag !== 'ok')
        throw Error('join lookup failed');
    let reply = first.val;
    if (reply.status === 404)
        reply = actors.call(directory, lookup);
    if (reply.status !== 200)
        throw Error(`join lookup answered ${reply.status}`);
    const r = new Reader(reply.body);
    directoryNumber(r);
    const entry = readEntry(r);
    directoryEnd(r);
    const slash = entry.target.indexOf('/');
    if (slash < 1)
        throw Error('invalid join target');
    if (after && (after.e > 9007199254740991n || after.n > 9007199254740991n || after.e < 0n || after.n < 0n))
        throw Error('invalid browser position');
    const path = `/?c=${encodeURIComponent(client)}&p=${p.toString(16).padStart(16, '0')}` + (after ? `&e=${after.e}&n=${after.n}` : '');
    return handOff(entry.target.slice(0, slash), entry.target.slice(slash + 1), path);
}
