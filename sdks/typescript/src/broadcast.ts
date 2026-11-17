/** Reconnecting room core. Export define() as an internal class. Fronts
 * authorize HTTP publish/history requests then use Client. Edges never call it. */
import { ScaleHost, type ScaleOptions } from "./broadcast-scaling";
import { Round } from "./broadcast-round";
import * as storage from "wasmkeep:actor/storage@0.2.0";
import * as alarms from "wasmkeep:actor/alarms@0.2.0";
import { Core, errorMessage, type Position, type Publish, type History, type Message } from "./broadcast-core";
import type { ActorDefinition, ActorRequest, ActorResponse } from "./index";
import { Reader, Writer, header } from "./wire";
export { Client } from "./broadcast-client";
export * from "./broadcast-core";
const PREFIX = "/wasmkeep-room/v1/", enc = new TextEncoder(), dec = new TextDecoder("utf-8", { fatal: true });
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
function serve(core: Core, op: string, body: Uint8Array): Uint8Array {
    const r = new Reader(body), w = new Writer();
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
    }
    else if (op === "history") {
        const after = flag(r) ? position(r) : undefined, limit = count(r, 2000), last = flag(r) ? count(r, 2000) : undefined;
        end(r);
        const h = core.history(after, limit, last);
        put(w, h.head);
        w.u64(h.lost ? 1 : 0);
        if (h.lost)
            w.u64(h.lost.e).u64(h.lost.base);
        w.u64(h.reset ? 1 : 0);
        if (h.reset)
            put(w, h.reset);
        w.u64(h.more ? 1 : 0).u64(h.messages.length);
        for (const m of h.messages) {
            put(w, m.position);
            w.u64(m.at).u64(m.id === undefined ? 0 : 1);
            if (m.id !== undefined)
                w.bytes(enc.encode(m.id));
            w.bytes(enc.encode(m.body));
        }
    }
    else
        throw Error("invalid room operation");
    return w.finish();
}
export function define(options: {
    confirm?: boolean;
    edges?: string[];
    scaling?: ScaleOptions;
} = {}): ActorDefinition {
    const core = new Core(storage, alarms, options.confirm);
    if (options.scaling && options.edges?.length)
        throw Error('invalid fixed and scaling edges');
    const round = new Round(options.edges ?? []);
    let scale: ScaleHost | undefined, lastRound: number | undefined;
    const scaler = () => scale ??= options.scaling ? new ScaleHost(options.scaling) : undefined;
    return { init() {
            core.init();
        }, alarm() {
            const now = Date.now();
            lastRound = now;
            if (options.scaling)
                scaler()!.run(round, core, now);
            else
                round.run(core, now);
        }, fetch(req: ActorRequest): ActorResponse {
            if (!header(req.headers, "x-wasmkeep-caller"))
                return { status: 403, body: "inside callers only" };
            if (req.method !== "POST")
                return { status: 405 };
            if (!req.path.startsWith(PREFIX))
                return { status: 404 };
            try {
                const body = req.bytes(270 * 1024);
                if (body === undefined)
                    throw Error("invalid body");
                if (req.path === PREFIX + 'poke') {
                    if (body.length)
                        throw Error('invalid poke');
                    if ((lastRound === undefined || Date.now() - lastRound > 1000) && alarms.getAlarm() === undefined)
                        alarms.setAlarm(BigInt(Date.now()));
                    return { status: 200 };
                }
                return { status: 200, body: serve(core, req.path.slice(PREFIX.length), body) };
            }
            catch (e) {
                return { status: errorMessage(e).includes("invalid") ? 400 : 503, body: errorMessage(e) };
            }
        } };
}
export { defineEdge } from "./broadcast-edge";
export type { ScaleOptions } from "./broadcast-scaling";
