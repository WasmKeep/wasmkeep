import { packRelay } from "./room-relay";
import { Suspect, type Relay } from "./room-scale";
import * as calls from "wasmkeep:actor/calls@0.2.0";
import * as actors from "wasmkeep:actor/actors@0.2.0";
import * as alarms from "wasmkeep:actor/alarms@0.2.0";
import type { Core, Position } from "./broadcast-core";
import type { Push } from "./room-edge";
import { encode } from "./room-utf8";
/** Message bytes one round builds for its children (ADR 0040, section 4). */
const ROUND_BYTES = 1024 * 1024;
/** What reading one row costs the round's budget besides its JSON: at
 * most about 1,000 rows a round, which an interpreter turns into items and
 * JSON well inside the default budget. */
const ROW_COST = 1024;
interface Slice {
    first?: number;
    messages: Push["messages"];
    sizes?: number[];
    recover: boolean;
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
}
export class Round {
    private children: Child[];
    private relays: Relay[] = [];
    /** The child the last round deferred first: the next round starts there. */
    private resume?: string;
    private suspects = new Map<string, Suspect>();
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
    run(core: Core, now: number) {
        for (const c of this.children)
            c.sockets = undefined;
        if (!core.beforePush(BigInt(now)))
            return;
        const epoch = core.wake(), epochs = core.epochs(), head = core.last() ?? epoch.base;
        const ready = this.children.filter(c => {
            const r = this.relayOf.get(c.address);
            return c.suspect.due <= now && !(r && (this.suspects.get(r.name)?.due ?? 0) > now);
        });
        // A continuation starts at the child the last round deferred first,
        // so under sustained publishing every child takes its turn.
        const from = Math.max(0, ready.findIndex(c => c.address === this.resume));
        const due = [...ready.slice(from), ...ready.slice(0, from)];
        this.resume = undefined;
        // Slices by start: children at one position share one read and one encoding.
        const slices = new Map<string, Slice>();
        let read = 0;
        const slice = (from: bigint, b: number): Slice | undefined => {
            const key = `${from}.${b}`;
            let s = slices.get(key);
            if (s)
                return s;
            // Reads stop with the budget too: a deferred child costs nothing.
            if (read >= ROUND_BYTES || budget <= 0)
                return;
            s = { messages: [], recover: false };
            // Never more rows than the budget has left (one at least).
            const rows = Math.max(1, Math.min(513, Math.floor((ROUND_BYTES - read) / ROW_COST)));
            for (const m of core.rows(from, rows, 512 * 1024)) {
                const boundary = epochs.find(e => e.e === m.position.e);
                if (!boundary) {
                    s = { messages: [], recover: true };
                    break;
                }
                const item = { e: number(m.position.e), b: number(boundary.base), a: number(m.at), i: m.id, d: m.body };
                // JSON escapes are ASCII: the UTF-8 size is the text's length plus each string's non-ASCII surplus.
                const bytes = JSON.stringify(item).length + (m.bodyBytes - m.body.length) + (m.id === undefined ? 0 : m.idBytes - m.id.length);
                if (!s.messages.length)
                    s.first = number(m.position.n);
                s.messages.push(item);
                s.sizes ??= [];
                s.sizes.push(bytes);
                // Every row costs, an empty one too: its lift, its item and its JSON.
                read += ROW_COST + bytes;
            }
            slices.set(key, s);
            return s;
        };
        // One round builds at most ROUND_BYTES of messages (one slice at
        // least): the rest keep their positions and go on 20 ms later.
        let budget = ROUND_BYTES, deferred = false;
        const defer = (c: Child) => {
            deferred = true;
            this.resume ??= c.address;
        };
        const sent: Child[] = [], pushes: Push[] = [], requests: [
            string,
            actors.Request
        ][] = [];
        for (const c of due) {
            let body: string, op: string;
            if (!c.initialized) {
                const a = actors.selfAddress();
                op = "init";
                body = JSON.stringify({ room: `${a.tenant}/${a.class}/${a.name}`, lo: "0000000000000000", state: "active" });
                pushes.push(JSON.parse(body));
            }
            else {
                op = "push";
                const p: Push = { e: number(epoch.e), b: number(epoch.base), first: number(head < 9007199254740991n ? head + 1n : head), head: number(head), messages: [] };
                if (c.position) {
                    p.b = number(epochs.filter(e => e.e > c.position!.e).reduce((b, e) => b < e.base ? b : e.base, epoch.base));
                    const n = epochs.filter(e => e.e > c.position!.e).reduce((n, e) => n < e.base ? n : e.base, c.position.n);
                    const s = n < head ? slice(n + 1n, p.b) : undefined;
                    if (n < head && !s) {
                        defer(c);
                        continue;
                    }
                    if (s?.recover)
                        p.recover = true;
                    else if (s?.messages.length) {
                        // The old cut: at most 512 messages and 512 KiB of JSON per request.
                        const fixed = JSON.stringify({ ...p, first: s.first, messages: [] }).length;
                        let k = 0, total = fixed;
                        while (k < s.messages.length && k < 512 && total + s.sizes![k] + (k ? 1 : 0) <= 512 * 1024)
                            total += s.sizes![k] + (k++ ? 1 : 0);
                        if (k && total - fixed > budget && budget < ROUND_BYTES) {
                            defer(c);
                            continue;
                        }
                        budget -= total - fixed;
                        p.first = s.first!;
                        p.messages = s.messages.slice(0, k);
                    }
                }
                pushes.push(p);
                body = JSON.stringify(p);
            }
            sent.push(c);
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
            next = now + 20;
        sent.forEach((c, i) => {
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
                    const a = JSON.parse(new TextDecoder().decode(r.val.body));
                    if (![a.e, a.n, a.lagging].every(n => Number.isSafeInteger(n) && n >= 0) || BigInt(a.e) > epoch.e || (BigInt(a.e) === epoch.e && BigInt(a.n) > head))
                        throw Error("invalid acknowledgment");
                    const position = { e: BigInt(a.e), n: BigInt(a.n) };
                    const stalledOnly = a.lagging > 0 && a.stalled === a.lagging && position.e === epoch.e && position.n === head;
                    const unchanged = c.position?.e === position.e && c.position?.n === position.n && c.stalled === a.stalled;
                    c.stallDelay = stalledOnly && unchanged ? Math.min(45000, (c.stallDelay ?? 10) * 2) : 20;
                    c.stalled = stalledOnly ? a.stalled : undefined;
                    c.position = position;
                    if (Number.isSafeInteger(a.sockets) && a.sockets >= 0)
                        c.sockets = a.sockets;
                    if (position.e !== epoch.e || position.n < head || a.lagging)
                        next = Math.min(next, now + (stalledOnly ? c.stallDelay : 20));
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
    }
}
