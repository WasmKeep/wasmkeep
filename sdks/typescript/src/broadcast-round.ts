import { Suspect } from "./room-scale";
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
}
export class Round {
    private children: Child[];
    constructor(addresses: string[]) {
        if (addresses.length > 16)
            throw Error("at most 16 fixed edges");
        this.children = addresses.map(address => ({ address, initialized: false, suspect: new Suspect() }));
    }
    sync(children: {
        name: string;
    }[]) {
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
        const due = this.children.filter(c => c.suspect.due <= now), enc = new TextEncoder();
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
        const results = calls.callMany(requests);
        due.forEach((c, i) => {
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
                    c.position = { e: BigInt(a.e), n: BigInt(a.n) };
                    if (Number.isSafeInteger(a.sockets) && a.sockets >= 0)
                        c.sockets = a.sockets;
                    if (c.position.e !== epoch.e || c.position.n < head || a.lagging)
                        next = Math.min(next, now + 20);
                }
                c.suspect.success();
            }
            catch {
                c.suspect.fail(now);
            }
        });
        for (const c of this.children)
            if (c.suspect.due > now)
                next = Math.min(next, c.suspect.due);
        if (next !== Infinity)
            alarms.setAlarm(BigInt(next));
    }
}
