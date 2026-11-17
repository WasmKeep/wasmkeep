import { Reader, Writer } from "./wire";
import { call } from "./call";
import type { Position, Publish, History, Message } from "./broadcast-core";
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
/** Front helper; no calls from an edge or relay. */
export class Client {
    constructor(readonly address: string) {
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
        end(r);
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
}
export { historyJSON } from "./room-history";
import * as calls from 'wasmkeep:actor/calls@0.2.0';
import * as actors from 'wasmkeep:actor/actors@0.2.0';
import { point, readEntry, number as directoryNumber, end as directoryEnd } from './directory-state';
import { handOff } from './request';
/** Authorize first. An empty room is created on its alarm, so a first join
 * can be unavailable until that alarm runs; the reconnecting client retries. */
export function join(room: string, directory: string, client: string, after?: Position) {
    const p = point(client), lookup = { method: 'POST', path: '/wasmkeep-directory/v1/lookup', headers: [] as [
            string,
            string
        ][], body: new Writer().u64(p).finish() };
    const results = calls.callMany([[directory, lookup], [room, { method: 'POST', path: PREFIX + 'poke', headers: [], body: new Uint8Array() }]]);
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
