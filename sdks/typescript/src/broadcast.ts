/** Reconnecting room core (ADR 0040). Export define() as an internal class
 * beside definePusher(). Fronts authorize HTTP publish/history requests
 * then use Client. The room sets no alarm and calls nobody, but on its
 * pusher's request while a 0.6.0 room moves over (section 13). */
import * as storage from "wasmkeep:actor/storage@0.2.0";
import * as actors from "wasmkeep:actor/actors@0.2.0";
import * as calls from "wasmkeep:actor/calls@0.2.0";
import { Core, errorMessage, type Position, type Publish } from "./broadcast-core";
import type { ActorDefinition, ActorRequest, ActorResponse } from "./index";
import { Reader, Writer, header } from "./wire";
export { Client } from "./broadcast-client";
export * from "./broadcast-core";
import { decode } from "./room-utf8";
const PREFIX = "/wasmkeep-room/v1/";
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
    return decode(b, { fatal: true });
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
const enc = new TextEncoder();
function legacy(table: string, column: string): Uint8Array | undefined {
    try {
        const v = storage.query(`SELECT ${column} FROM ${table} LIMIT 1`, []).values[0]?.[0];
        return v?.tag === "text" ? enc.encode(v.val) : v?.tag === "blob" ? v.val : undefined;
    }
    catch {
        return undefined; // No such table: a room that never scaled.
    }
}
/** `adopt` (section 13): a 0.6.0 room's scale rows, read, handed over and
 * retired at its pusher's request. */
function adopt(r: Reader, w: Writer, pusher: string) {
    const kind = count(r, 2);
    if (kind === 0) {
        end(r);
        const data = legacy("wasmkeep_room_scale", "data"), op = data && legacy("wasmkeep_room_ops", "args");
        w.u64(data ? 1 : 0);
        if (data)
            w.bytes(data);
        w.u64(op ? 1 : 0);
        if (op)
            w.bytes(op);
        return;
    }
    if (kind === 2) {
        end(r);
        storage.exec("BEGIN", []);
        try {
            storage.exec("DROP TABLE IF EXISTS wasmkeep_room_ops", []);
            storage.exec("DROP TABLE IF EXISTS wasmkeep_room_scale", []);
            storage.execDurable("COMMIT", []);
        }
        catch (e) {
            try {
                storage.exec("ROLLBACK", []);
            }
            catch {
            }
            throw e;
        }
        return;
    }
    const directory = text(r), n = count(r, 16), edges: string[] = [];
    for (let i = 0; i < n; i++)
        edges.push(text(r));
    end(r);
    // This room is the controller the map and the edges still accept.
    if (directory) {
        const t = actors.call(directory, { method: "POST", path: "/wasmkeep-directory/v1/transfer", headers: [], body: new Writer().bytes(enc.encode(pusher)).finish() });
        if (t.status !== 200 && t.status !== 404)
            throw Error(`directory transfer answered ${t.status}`);
    }
    const body = enc.encode(JSON.stringify({ room: pusher }));
    for (const result of calls.callMany(edges.map(e => [e, { method: "POST", path: "/wasmkeep-edge/v1/handover", headers: [], body }])))
        if (result.tag !== "ok" || result.val.status !== 200)
            throw Error("edge handover failed");
}
/** The room's operations, as its handler serves them (exported for tests). */
export function serve(core: Core, op: string, body: Uint8Array, caller: string, pusher: string): Uint8Array {
    const r = new Reader(body), w = new Writer();
    if ((op === "pull" || op === "adopt") && caller !== pusher)
        throw Error("forbidden: the room's pusher only");
    if (op === "publish") {
        const n = count(r, 64), messages: Publish[] = [];
        for (let i = 0; i < n; i++) {
            const id = flag(r) ? text(r) : undefined;
            messages.push({ id, body: text(r) });
        }
        end(r);
        const result = core.publish(messages, BigInt(Date.now()));
        w.u64(result.length);
        for (const p of result)
            put(w, p);
        // Section 13: ask the front to poke the pusher only when it went idle.
        w.u64(core.poke(Date.now()) ? 1 : 0);
    }
    else if (op === "pull") {
        const after = flag(r) ? position(r) : undefined, budget = count(r, 1024 * 1024), idle = flag(r);
        end(r);
        const { pull: p, raw } = core.pull(after, budget, idle, Date.now(), true);
        w.u64(p.e).u64(p.b).u64(p.first).u64(p.head).u64(p.recover ? 1 : 0).u64(p.messages.length);
        p.messages.forEach((m, i) => {
            put(w, m.position);
            w.u64(m.b).u64(m.at).u64(m.id === undefined ? 0 : 1);
            if (m.id !== undefined)
                w.bytes(raw[i].id!);
            w.bytes(raw[i].body);
        });
    }
    else if (op === "adopt")
        adopt(r, w, pusher);
    else if (op === "history") {
        const after = flag(r) ? position(r) : undefined, limit = count(r, 2000), last = flag(r) ? count(r, 2000) : undefined;
        end(r);
        const { history: h, raw } = core.page(after, limit, last, true);
        put(w, h.head);
        w.u64(h.lost ? 1 : 0);
        if (h.lost)
            w.u64(h.lost.e).u64(h.lost.base);
        w.u64(h.reset ? 1 : 0);
        if (h.reset)
            put(w, h.reset);
        w.u64(h.more ? 1 : 0).u64(h.messages.length);
        h.messages.forEach((m, i) => {
            put(w, m.position);
            w.u64(m.at).u64(m.id === undefined ? 0 : 1);
            if (m.id !== undefined)
                w.bytes(raw[i].id!);
            w.bytes(raw[i].body);
        });
    }
    else
        throw Error("invalid room operation");
    return w.finish();
}
export interface RoomOptions {
    confirm?: boolean;
    /** The pusher's class (section 13): the room `room/lobby` has the
     * pusher `<pusher>/lobby`. Default `pusher`. */
    pusher?: string;
}
export function define(options: RoomOptions = {}): ActorDefinition {
    if ("edges" in options || "scaling" in options)
        throw Error("invalid room options: edges and scaling moved to broadcast.definePusher() (ADR 0040, section 13)");
    const core = new Core(storage, options.confirm);
    let pusher: string | undefined;
    const pusherAddress = () => {
        if (pusher === undefined) {
            const a = actors.selfAddress();
            pusher = `${a.tenant}/${options.pusher ?? "pusher"}/${a.name}`;
        }
        return pusher;
    };
    return { init() {
            core.init();
        }, alarm() {
            // A 0.6.0 room's alarm fires once more: rounds are the pusher's now.
        }, fetch(req: ActorRequest): ActorResponse {
            const caller = header(req.headers, "x-wasmkeep-caller");
            if (!caller)
                return { status: 403, body: "inside callers only" };
            if (req.method !== "POST")
                return { status: 405 };
            if (!req.path.startsWith(PREFIX))
                return { status: 404 };
            try {
                const body = req.bytes(270 * 1024);
                if (body === undefined)
                    throw Error("invalid body");
                // A 0.6.0 front still pokes the room: the pusher is poked by the front now.
                if (req.path === PREFIX + 'poke')
                    return { status: 200 };
                return { status: 200, body: serve(core, req.path.slice(PREFIX.length), body, caller, pusherAddress()) };
            }
            catch (e) {
                const m = errorMessage(e);
                return { status: m.includes("invalid") ? 400 : m.startsWith("forbidden") ? 403 : 503, body: m };
            }
        } };
}
export { defineEdge } from "./broadcast-edge";
export type { ScaleOptions } from "./broadcast-scaling";
export { defineRelay } from './broadcast-relay';
export { definePusher, Pulled, type PusherOptions } from './broadcast-pusher';
export { Round, local, type Source } from './broadcast-round';
