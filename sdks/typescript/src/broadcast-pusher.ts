/** The pusher (ADR 0040, section 13): the room's rounds, children, relays
 * and scaling, in a class of their own. Export definePusher() as an
 * internal class beside the room's define(). It pulls from the room and
 * pushes to relays and edges; the room never calls it. */
import * as alarms from "wasmkeep:actor/alarms@0.2.0";
import * as actors from "wasmkeep:actor/actors@0.2.0";
import { call } from "./call";
import { Round, type Source } from "./broadcast-round";
import { ScaleHost, type ScaleOptions } from "./broadcast-scaling";
import { errorMessage, type Position, type Pull } from "./broadcast-core";
import type { ActorDefinition, ActorRequest, ActorResponse } from "./index";
import { Reader, Writer, header } from "./wire";
import { decode } from "./room-utf8";
const PREFIX = "/wasmkeep-room/v1/";
const POKE = "/wasmkeep-pusher/v1/poke";
function number(r: Reader): bigint {
    const n = r.u64();
    if (n === undefined)
        throw Error("invalid pull answer");
    return n;
}
function flag(r: Reader): boolean {
    const n = number(r);
    if (n > 1n)
        throw Error("invalid pull answer");
    return n === 1n;
}
function bytes(r: Reader): Uint8Array {
    const b = r.bytes();
    if (b === undefined)
        throw Error("invalid pull answer");
    return b;
}
/** The room's `pull` and `adopt`, over calls: the pusher's `Source`. */
export class Pulled implements Source {
    constructor(readonly room: string) {
    }
    private raw(op: string, body: Uint8Array): Uint8Array {
        const result = call(this.room, "POST", PREFIX + op, [], body);
        if ("error" in result)
            throw Error(result.error);
        if (result.reply.status !== 200)
            throw Error(`room answered ${result.reply.status}: ${new TextDecoder().decode(result.reply.body.slice(0, 200))}`);
        return result.reply.body;
    }
    private call(op: string, body: Uint8Array): Reader {
        return new Reader(this.raw(op, body));
    }
    pull(after: Position | undefined, budget: number, idle: boolean): Pull {
        const w = new Writer().u64(after ? 1 : 0);
        if (after)
            w.u64(after.e).u64(after.n);
        const r = this.call("pull", w.u64(budget).u64(idle ? 1 : 0).finish());
        const p: Pull = { e: number(r), b: number(r), first: number(r), head: number(r), recover: flag(r), messages: [] };
        const n = number(r);
        if (n > 513n)
            throw Error("invalid pull answer");
        for (let i = 0n; i < n; i++) {
            const position = { e: number(r), n: number(r) }, b = number(r), at = number(r);
            const id = flag(r) ? bytes(r) : undefined, body = bytes(r);
            if (position.n !== p.first + i)
                throw Error("invalid pull answer");
            const m = { position, b, at, id: id && decode(id, { fatal: true }), body: decode(body, { fatal: true }), bodyBytes: body.length, idBytes: id?.length ?? 0 };
            p.messages.push(m);
        }
        if (!r.isEmpty())
            throw Error("invalid pull answer");
        return p;
    }
    /** Section 13's migration steps: 0 reads, 1 hands over, 2 retires. */
    adopt(kind: number, directory = "", edges: string[] = []): Uint8Array {
        const w = new Writer().u64(kind);
        if (kind === 1) {
            const enc = new TextEncoder();
            w.bytes(enc.encode(directory)).u64(edges.length);
            for (const e of edges)
                w.bytes(enc.encode(e));
        }
        return this.raw("adopt", w.finish());
    }
}
export interface PusherOptions {
    /** The room's class: the pusher `pusher/lobby` pulls from `<room>/lobby`.
     * Default `room`. */
    room?: string;
    /** Fixed edges (ADR 0040, R3), or */
    edges?: string[];
    /** the scaled group (R4/R5). */
    scaling?: ScaleOptions;
}
/** After a round whose next alarm is not 20 ms away, the pusher tells the
 * room it stops clocking itself: the room then asks publishers' fronts to
 * poke it. A head that moved meanwhile keeps the 20 ms clock instead. */
/** A pusher that went idle after delivering something new keeps one alarm
 * this much later: a poke lost while it could not be reached (its node
 * died between a publish and its round) is caught up then. An idle round
 * that finds nothing new sets none, so a quiet room keeps no alarm. */
export const SAFETY_ROUND = 5000;
function idle(source: Source, at: Position | undefined, next: number | undefined, now: number, safe: { head?: Position }) {
    if (next !== undefined && next <= now + 20)
        return;
    const later = (t: number) => next === undefined || next > t;
    try {
        const h = source.pull(at, 0, true, now);
        if (at && (h.e !== at.e || h.head !== at.n)) {
            if (later(now + 20))
                alarms.setAlarm(BigInt(now + 20));
        }
        else if (at && (safe.head?.e !== at.e || safe.head?.n !== at.n)) {
            safe.head = at;
            if (later(now + SAFETY_ROUND))
                alarms.setAlarm(BigInt(now + SAFETY_ROUND));
        }
    }
    catch {
        // The room is unreachable: it cannot ask for a poke, so keep a clock.
        if (later(now + 1000))
            alarms.setAlarm(BigInt(now + 1000));
    }
}
export function definePusher(options: PusherOptions = {}): ActorDefinition {
    if (options.scaling && options.edges?.length)
        throw Error("invalid fixed and scaling edges");
    const round = new Round(options.edges ?? []);
    let scale: ScaleHost | undefined, source: Pulled | undefined, lastRound: number | undefined;
    const safe: { head?: Position } = {};
    const room = () => {
        if (!source) {
            const a = actors.selfAddress();
            source = new Pulled(`${options.room ?? "room"}/${a.name}`);
        }
        return source;
    };
    const scaler = () => scale ??= options.scaling ? new ScaleHost(options.scaling) : undefined;
    return {
        init() {
            scaler()?.init();
        },
        alarm() {
            const now = Date.now();
            lastRound = now;
            const s = scaler();
            if (s && !s.migrate(room(), now))
                return;
            const result = s ? s.run(round, room(), now) : round.run(room(), now);
            idle(room(), result.delivered, result.next, now, safe);
        },
        fetch(req: ActorRequest): ActorResponse {
            if (!header(req.headers, "x-wasmkeep-caller"))
                return { status: 403, body: "inside callers only" };
            if (req.method !== "POST")
                return { status: 405 };
            if (req.path !== POKE)
                return { status: 404 };
            try {
                const body = req.bytes(64);
                if (body === undefined)
                    throw Error("invalid poke");
                const now = Date.now(), at = alarms.getAlarm();
                if (body.length === 0) {
                    // A join's poke: a round now, at most one a second.
                    if ((lastRound === undefined || now - lastRound > 1000) && at == null)
                        alarms.setAlarm(BigInt(now));
                }
                else {
                    const r = new Reader(body);
                    if (r.u64() !== 1n || !r.isEmpty())
                        throw Error("invalid poke");
                    // A publish's poke: the round 20 ms from now, as a publish did before.
                    if (at == null || at > BigInt(now + 20))
                        alarms.setAlarm(BigInt(now + 20));
                }
                return { status: 200 };
            }
            catch (e) {
                return { status: 400, body: errorMessage(e) };
            }
        },
    };
}
