import { packRelay } from "./room-relay";
import { Suspect, type Relay } from "./room-scale";
import * as calls from "wasmkeep:actor/calls@0.2.0";
import * as actors from "wasmkeep:actor/actors@0.2.0";
import * as alarms from "wasmkeep:actor/alarms@0.2.0";
import { Core, errorMessage, ROW_COST, type Position, type Pull } from "./broadcast-core";
import type { Push } from "./room-edge";
import { encode } from "./room-utf8";
/** Message bytes one round builds for its children (ADR 0040, section 4). */
const ROUND_BYTES = 1024 * 1024;
/** What a round reads from: the room's `pull` (a `Pulled` source in the
 * pusher), or a `Core` in this instance (`local`). */
export interface Source {
    pull(after: Position | undefined, budget: number, idle: boolean, now: number): Pull;
}
/** A `Core` as a source, in one instance: tests, and the shared vectors
 * run both ways. */
export function local(core: Core): Source {
    return { pull: (after, budget, idle, now) => core.pull(after, budget, idle, now).pull };
}
/** A source, or a `Core` taken as `local(core)`. */
export function asSource(from: Source | Core): Source {
    return from instanceof Core ? local(from) : from;
}
export interface RoundResult {
    /** The alarm the round set, if any. */
    next?: number;
    /** The lowest head a pushed child was given: where an idle pull claims to be. */
    delivered?: Position;
    /** Some child holds sockets, or did not say: an idle pusher keeps an alarm. */
    occupied?: boolean;
    /** Every child is initialized and acknowledged this one position: an
     * idle pusher keeps it, for its next cold start (`Round.wakeAt`). */
    settled?: Position;
}
/** Distinct positions one round pulls at most: the rest are deferred to
 * the next round, so laggards at many positions never make one round a
 * long chain of synchronous calls to the room. */
export const MAX_PULLS = 16;
/** The work one round does at most, in counts a guest can keep (ADR 0040,
 * section 13): each pull costs `PULL_WORK`, each request to a child
 * `PUSH_WORK`, each row read 1 and each row encoded into a child's push
 * 1 (once per child). The children the rest would need are deferred to
 * the next round, which starts at the first of them. Chosen so a round
 * stays near half the 50 ms budget on QuickJS whatever the children lag. */
export const ROUND_WORK = 640;
/** A pull's work: one call to the room and its header. */
const PULL_WORK = 8;
/** A request's work: one child's push or init, its call and its answer. */
const PUSH_WORK = 3;
interface Slice {
    header: Pull;
    first?: number;
    messages: Push["messages"];
    sizes?: number[];
    recover: boolean;
    /** Relayed: the rows and JSON bytes of this slice already counted,
     * since a relay's children share one encoding of them. */
    charged?: number;
    chargedBytes?: number;
    /** The cuts made of it, shared by the children given the same rows. */
    cuts?: Map<number, Push["messages"]>;
    fixed?: number;
}
const number = (v: bigint) => {
    const n = Number(v);
    if (!Number.isSafeInteger(n) || n < 0 || BigInt(n) !== v)
        throw Error("invalid browser integer");
    return n;
};
interface Child {
    address: string;
    position?: Position;
    initialized: boolean;
    suspect: Suspect;
    sockets?: number;
    stallDelay?: number;
    stalled?: number;
    /** The delay its last answer asked for (lagging sockets, or behind):
     * a child left out keeps it. */
    retry?: number;
}
export class Round {
    private children: Child[];
    private relays: Relay[] = [];
    /** The child the last round deferred first: the next round starts there. */
    private resume?: string;
    private suspects = new Map<string, Suspect>();
    /** Where every child stood when this pusher last went idle, read back
     * after a cold start: the first run gives it to the children that have
     * no position (`wakeAt`). */
    private wake?: Position;
    /** The position the first run gave, until a pull refuses it. */
    private seeded?: Position;
    constructor(addresses: string[]) {
        if (addresses.length > 16)
            throw Error("at most 16 fixed edges");
        this.children = addresses.map(address => ({ address, initialized: false, suspect: new Suspect() }));
        this.byAddress = new Map(this.children.map(c => [c.address, c]));
    }
    sync(children: {
        name: string;
    }[], relays: Relay[] = []) {
        // Maps, not nested scans: a room has up to 256 children every round.
        this.relays = relays;
        this.relayOf = new Map(relays.flatMap(r => r.edges.map(e => [e, r] as const)));
        const relayNames = new Set(relays.map(r => r.name));
        for (const name of this.suspects.keys())
            if (!relayNames.has(name))
                this.suspects.delete(name);
        const names = new Set(children.map(c => c.name));
        this.children = this.children.filter(c => names.has(c.address));
        const have = new Set(this.children.map(c => c.address));
        for (const c of children)
            if (!have.has(c.name))
                this.children.push({ address: c.name, initialized: true, suspect: new Suspect() });
        this.byAddress = new Map(this.children.map(c => [c.address, c]));
    }
    private relayOf = new Map<string, Relay>();
    private byAddress = new Map<string, Child>();
    count(name: string) { return this.byAddress.get(name)?.sockets; }
    /** After a cold start: where every child stood when this pusher last
     * went idle (ADR 0040, section 13). The next run gives it to each child
     * that has no position, so a quiet room's first publish after a full
     * hibernation is pushed to the edges, not announced as a header every
     * client would then fetch over HTTP. A child that was elsewhere answers
     * its own position, as after any push. */
    wakeAt(p: Position) { this.wake = p; }
    /** One round over `source` (the room's `pull`, or a local `Core`).
     * Sets the next alarm when one is due and answers it, with the
     * position every pushed child was brought to (the lowest header
     * used), for the pusher's idle pull. */
    run(input: Source | Core, now: number): RoundResult {
        const source = asSource(input);
        if (this.wake) {
            for (const c of this.children)
                c.position ??= this.wake;
            this.seeded = this.wake;
            this.wake = undefined;
        }
        let refused = false;
        const ready = this.children.filter(c => {
            const r = this.relayOf.get(c.address);
            const due = c.suspect.due <= now && !(r && (this.suspects.get(r.name)?.due ?? 0) > now);
            if (!due)
                c.sockets = undefined;
            return due;
        });
        // A continuation starts at the child the last round deferred first,
        // so under sustained publishing every child takes its turn.
        const from = Math.max(0, ready.findIndex(c => c.address === this.resume));
        const due = [...ready.slice(from), ...ready.slice(0, from)];
        this.resume = undefined;
        // Pulls by position: children at one position share one pull and one encoding.
        const slices = new Map<string, Slice | undefined>();
        const keyOf = (position: Position | undefined) => position ? `${position.e}.${position.n}` : "";
        // Relayed, a slice's rows are encoded once for all its children.
        const relayed = this.relays.length > 0;
        // The children at each position: a pull is sized to serve them all.
        const sharers = new Map<string, number>();
        for (const c of due)
            if (c.initialized)
                sharers.set(keyOf(c.position), (sharers.get(keyOf(c.position)) ?? 0) + 1);
        // The round's work in counts (ROUND_WORK): pulls, requests and rows.
        let read = 0, pulls = 0, work = 0, retry = 20, delivered: Position | undefined, current: Pull | undefined;
        const slice = (position: Position | undefined): Slice | undefined => {
            const key = keyOf(position);
            if (slices.has(key))
                return slices.get(key);
            // A child at a head the round already pulled needs no read.
            if (position && current && position.e === current.e && position.n === current.head)
                return { header: { ...current, first: current.head + 1n, recover: false, messages: [] }, messages: [], recover: false };
            // Reads stop with the budget too: a deferred child costs nothing.
            if (position && (read >= ROUND_BYTES || budget <= 0))
                return;
            if (pulls >= MAX_PULLS || work + PULL_WORK + PUSH_WORK + (position ? 2 : 0) > ROUND_WORK)
                return;
            pulls++;
            // What the pull reads is pushed to the children at its position:
            // the work left after their requests buys rows, each counted
            // once read and once pushed (once per child unless relayed).
            const n = sharers.get(key) ?? 1;
            const rows = Math.max(1, Math.floor((ROUND_WORK - work - PULL_WORK - PUSH_WORK * n) / (relayed ? 2 : 1 + n)));
            work += PULL_WORK;
            let pull: Pull;
            try {
                pull = source.pull(position, position ? Math.min(ROUND_BYTES - read, rows * ROW_COST) : 0, false, now);
            }
            catch (e) {
                // Not durable yet (confirm): the upload's retry is 50 ms.
                if (errorMessage(e).includes("not durable"))
                    retry = 50;
                // A position read back that the room no longer accepts:
                // those children start with none.
                if (errorMessage(e).includes("invalid position") && position && this.seeded && position.e === this.seeded.e && position.n === this.seeded.n)
                    refused = true;
                slices.set(key, undefined);
                return;
            }
            // The header of a current-epoch position (or none) carries the epoch's own base.
            if (!position || position.e === pull.e)
                current = pull;
            if (!delivered || pull.e < delivered.e || (pull.e === delivered.e && pull.head < delivered.n))
                delivered = { e: pull.e, n: pull.head };
            const s: Slice = { header: pull, messages: [], sizes: [], recover: pull.recover };
            for (const m of pull.messages) {
                const item = { e: number(m.position.e), b: number(m.b), a: number(m.at), i: m.id, d: m.body };
                // JSON escapes are ASCII: the UTF-8 size is the text's length plus each string's non-ASCII surplus.
                const bytes = JSON.stringify(item).length + (m.bodyBytes - m.body.length) + (m.id === undefined ? 0 : m.idBytes - m.id.length);
                if (!s.messages.length)
                    s.first = number(m.position.n);
                s.messages.push(item);
                s.sizes!.push(bytes);
                // Every row costs, an empty one too: its lift, its item and its JSON.
                read += ROW_COST + bytes;
            }
            work += s.messages.length;
            slices.set(key, s);
            return s;
        };
        // One round builds at most ROUND_BYTES of messages (one slice at
        // least): the rest keep their positions and go on 20 ms later.
        let budget = ROUND_BYTES, deferred = false, held = Infinity;
        const defer = (c: Child) => {
            deferred = true;
            this.resume ??= c.address;
        };
        const sent: Child[] = [], pushes: Push[] = [], heads: Pull[] = [], requests: [
            string,
            actors.Request
        ][] = [];
        for (const c of due) {
            let body: string, op: string, header: Pull | undefined;
            if (work + PUSH_WORK > ROUND_WORK) {
                // A child at the head this round read needs no messages: it
                // goes first next round, and sets no alarm unless its last
                // answer asked for one (its sockets lagging), so a group at
                // its head goes idle. It keeps its last socket count.
                if (c.initialized && c.position && current && c.position.e === current.e && c.position.n === current.head) {
                    this.resume ??= c.address;
                    if (c.retry !== undefined)
                        held = Math.min(held, now + c.retry);
                }
                else
                    defer(c);
                continue;
            }
            if (!c.initialized) {
                work += PUSH_WORK;
                const a = actors.selfAddress();
                op = "init";
                body = JSON.stringify({ room: `${a.tenant}/${a.class}/${a.name}`, lo: "0000000000000000", state: "active" });
                pushes.push(JSON.parse(body));
            }
            else {
                op = "push";
                const s = slice(c.position);
                if (!s) {
                    defer(c);
                    continue;
                }
                const h = header = s.header;
                const p: Push = { e: number(h.e), b: number(h.b), first: number(h.head < 9007199254740991n ? h.head + 1n : h.head), head: number(h.head), messages: [] };
                // Every row encoded counts, once per child it is pushed to,
                // or once per slice when relayed. Its bytes too.
                const charged = relayed ? s.charged ?? 0 : 0, chargedBytes = relayed ? s.chargedBytes ?? 0 : 0;
                const rows = Math.max(0, ROUND_WORK - work - PUSH_WORK);
                let fresh = 0;
                if (s.recover)
                    p.recover = true;
                else if (s.messages.length) {
                    if (!rows && !charged) {
                        defer(c);
                        continue;
                    }
                    // The old cut: at most 512 messages and 512 KiB of JSON
                    // per request, and the round's rows left.
                    // The header's JSON is the same for every child of a slice.
                    const fixed = s.fixed ??= JSON.stringify({ ...p, first: s.first, messages: [] }).length;
                    let k = 0, bytes = 0;
                    while (k < s.messages.length && k < 512 && k < charged + rows && fixed + bytes + s.sizes![k] + (k ? 1 : 0) <= 512 * 1024)
                        bytes += s.sizes![k] + (k++ ? 1 : 0);
                    const more = Math.max(0, bytes - chargedBytes);
                    if (k > charged && more > budget && budget < ROUND_BYTES) {
                        defer(c);
                        continue;
                    }
                    budget -= more;
                    fresh = Math.max(0, k - charged);
                    if (relayed && k > charged) {
                        s.charged = k;
                        s.chargedBytes = bytes;
                    }
                    p.first = s.first!;
                    s.cuts ??= new Map();
                    p.messages = s.cuts.get(k) ?? s.messages.slice(0, k);
                    s.cuts.set(k, p.messages);
                }
                work += PUSH_WORK + fresh;
                pushes.push(p);
                // Relayed, the relay's pack is the only encoding.
                body = relayed ? "" : JSON.stringify(p);
            }
            sent.push(c);
            heads.push(header!);
            requests.push([c.address, { method: "POST", path: `/wasmkeep-edge/v1/${op}`, headers: [], body: this.relays.length ? new Uint8Array() : encode(body) }]);
        }
        let next = Infinity;
        const skipped = new Set<number>();
        let results: ReturnType<typeof calls.callMany>;
        if (!this.relays.length)
            results = calls.callMany(requests);
        else {
            results = requests.map(() => ({ tag: 'err', val: { tag: 'timeout' } }));
            const byRelay = new Map<Relay, number[]>();
            sent.forEach((c, i) => {
                const r = this.relayOf.get(c.address);
                if (r)
                    byRelay.set(r, [...(byRelay.get(r) ?? []), i]);
            });
            const groups = [...byRelay].map(([relay, indices]) => ({ relay, indices }));
            const replies = calls.callMany(groups.map(g => [g.relay.name, { method: 'POST', path: '/wasmkeep-relay/v1/push', headers: [], body: encode(JSON.stringify(packRelay(g.indices.map(i => ({ edge: sent[i].address, push: pushes[i] }))))) }]));
            groups.forEach((g, i) => {
                const suspect = this.suspects.get(g.relay.name) ?? new Suspect();
                this.suspects.set(g.relay.name, suspect);
                try {
                    const reply = replies[i];
                    if (reply.tag !== 'ok' || reply.val.status !== 200)
                        throw Error('relay failed');
                    const answers = JSON.parse(new TextDecoder().decode(reply.val.body));
                    if (!Array.isArray(answers) || answers.length !== g.indices.length)
                        throw Error('invalid relay reply');
                    suspect.success();
                    g.indices.forEach((index, j) => { results[index] = { tag: 'ok', val: { status: answers[j] === null ? 503 : 200, headers: [], body: new TextEncoder().encode(JSON.stringify(answers[j])) } }; });
                }
                catch {
                    suspect.fail(now);
                    g.indices.forEach(index => skipped.add(index));
                }
            });
        }
        if (deferred)
            next = now + retry;
        next = Math.min(next, held);
        sent.forEach((c, i) => {
            // A child sent a request this round says its count and retry again.
            c.sockets = undefined;
            c.retry = undefined;
            if (skipped.has(i))
                return; // Relay failure belongs to the relay, not its edges.
            try {
                const r = results[i];
                if (r.tag !== "ok" || r.val.status !== 200)
                    throw Error("failed push");
                if (!c.initialized) {
                    c.initialized = true;
                    next = Math.min(next, now + 20);
                }
                else {
                    // Each child's answer is checked against its own pull's header.
                    const { e: epoch, head } = heads[i];
                    const a = JSON.parse(new TextDecoder().decode(r.val.body));
                    if (![a.e, a.n, a.lagging].every(n => Number.isSafeInteger(n) && n >= 0) || BigInt(a.e) > epoch || (BigInt(a.e) === epoch && BigInt(a.n) > head))
                        throw Error("invalid acknowledgment");
                    const position = { e: BigInt(a.e), n: BigInt(a.n) };
                    const stalledOnly = a.lagging > 0 && a.stalled === a.lagging && position.e === epoch && position.n === head;
                    const unchanged = c.position?.e === position.e && c.position?.n === position.n && c.stalled === a.stalled;
                    c.stallDelay = stalledOnly && unchanged ? Math.min(45000, (c.stallDelay ?? 10) * 2) : 20;
                    c.stalled = stalledOnly ? a.stalled : undefined;
                    c.position = position;
                    if (Number.isSafeInteger(a.sockets) && a.sockets >= 0)
                        c.sockets = a.sockets;
                    if (position.e !== epoch || position.n < head || a.lagging) {
                        c.retry = stalledOnly ? c.stallDelay : 20;
                        next = Math.min(next, now + c.retry);
                    }
                }
                c.suspect.success();
            }
            catch {
                c.suspect.fail(now);
            }
        });
        for (const s of this.suspects.values())
            if (s.due > now)
                next = Math.min(next, s.due);
        for (const c of this.children)
            if (c.suspect.due > now)
                next = Math.min(next, c.suspect.due);
        if (next !== Infinity)
            alarms.setAlarm(BigInt(next));
        if (refused) {
            const s = this.seeded!;
            for (const c of this.children)
                if (c.position && c.position.e === s.e && c.position.n === s.n)
                    c.position = undefined;
            this.seeded = undefined;
        }
        const occupied = this.children.some(c => c.sockets === undefined || c.sockets > 0);
        // Every child initialized and acknowledged at one position: where an
        // idle pusher may say they all stand.
        const p = this.children[0]?.position;
        const settled = p && this.children.every(c => c.initialized && c.position?.e === p.e && c.position.n === p.n) ? { ...p } : undefined;
        return { next: next === Infinity ? undefined : next, delivered, occupied, settled };
    }
}
