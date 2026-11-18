import { packRelay } from "./room-relay";
import { Suspect, type Relay } from "./room-scale";
import * as calls from "wasmkeep:actor/calls@0.2.0";
import * as actors from "wasmkeep:actor/actors@0.2.0";
import * as alarms from "wasmkeep:actor/alarms@0.2.0";
import type { Core, Position } from "./broadcast-core";
import type { Push } from "./room-edge";
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
    private suspects = new Map<string, Suspect>();
    constructor(addresses: string[]) {
        if (addresses.length > 16)
            throw Error("at most 16 fixed edges");
        this.children = addresses.map(address => ({ address, initialized: false, suspect: new Suspect() }));
    }
    sync(children: {
        name: string;
    }[], relays: Relay[] = []) {
        this.relays = relays;
        for (const name of this.suspects.keys())
            if (!relays.some(r => r.name === name))
                this.suspects.delete(name);
        this.children = this.children.filter(c => children.some(n => n.name === c.address));
        for (const c of children)
            if (!this.children.some(n => n.address === c.name))
                this.children.push({ address: c.name, initialized: true, suspect: new Suspect() });
    }
    count(name: string) { return this.children.find(c => c.address === name)?.sockets; }
    run(core: Core, now: number) {
        for (const c of this.children)
            c.sockets = undefined;
        if (!core.beforePush(BigInt(now)))
            return;
        const epoch = core.wake(), rows = core.messages(), epochs = core.epochs(), head = rows.at(-1)?.position.n ?? epoch.base;
        const due = this.children.filter(c => c.suspect.due <= now && !this.relays.some(r => r.edges.includes(c.address) && (this.suspects.get(r.name)?.due ?? 0) > now)), enc = new TextEncoder();
        const requests: [
            string,
            actors.Request
        ][] = due.map(c => {
            let body: string, op: string;
            if (!c.initialized) {
                const a = actors.selfAddress();
                op = "init";
                body = JSON.stringify({ room: `${a.tenant}/${a.class}/${a.name}`, lo: "0000000000000000", state: "active" });
            }
            else {
                op = "push";
                const p: Push = { e: number(epoch.e), b: number(epoch.base), first: number(head < 9007199254740991n ? head + 1n : head), head: number(head), messages: [] };
                if (c.position) {
                    p.b = number(epochs.filter(e => e.e > c.position!.e).reduce((b, e) => b < e.base ? b : e.base, epoch.base));
                    const n = epochs.filter(e => e.e > c.position!.e).reduce((n, e) => n < e.base ? n : e.base, c.position.n);
                    for (const m of rows.filter(m => m.position.n > n)) {
                        const boundary = epochs.find(e => e.e === m.position.e);
                        if (!boundary) {
                            p.recover = true;
                            p.messages = [];
                            break;
                        }
                        if (!p.messages.length)
                            p.first = number(m.position.n);
                        p.messages.push({ e: number(m.position.e), b: number(boundary.base), a: number(m.at), i: m.id, d: m.body });
                        if (p.messages.length > 512 || enc.encode(JSON.stringify(p)).length > 512 * 1024) {
                            p.messages.pop();
                            break;
                        }
                    }
                }
                body = JSON.stringify(p);
            }
            return [c.address, { method: "POST", path: `/wasmkeep-edge/v1/${op}`, headers: [], body: enc.encode(body) }];
        });
        let next = Infinity;
        const skipped = new Set<number>();
        let results: ReturnType<typeof calls.callMany>;
        if (!this.relays.length)
            results = calls.callMany(requests);
        else {
            results = requests.map(() => ({ tag: 'err', val: { tag: 'timeout' } }));
            const groups = this.relays.map(r => ({ relay: r, indices: due.map((c, i) => r.edges.includes(c.address) ? i : -1).filter(i => i >= 0) })).filter(g => g.indices.length);
            const replies = calls.callMany(groups.map(g => [g.relay.name, { method: 'POST', path: '/wasmkeep-relay/v1/push', headers: [], body: enc.encode(JSON.stringify(packRelay(g.indices.map(i => ({ edge: due[i].address, push: JSON.parse(new TextDecoder().decode(requests[i][1].body)) }))))) }]));
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
                    g.indices.forEach((index, j) => { results[index] = { tag: 'ok', val: { status: answers[j] === null ? 503 : 200, headers: [], body: enc.encode(JSON.stringify(answers[j])) } }; });
                }
                catch {
                    suspect.fail(now);
                    g.indices.forEach(index => skipped.add(index));
                }
            });
        }
        due.forEach((c, i) => {
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
