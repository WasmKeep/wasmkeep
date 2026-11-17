/** ADR 0040 R4/R5: deterministic scaling and a durable, replayable operation. */
export interface Child {
    name: string;
    lo: bigint;
    hi?: bigint;
    sockets?: number;
}
export interface Decision {
    kind: 'split' | 'merge';
    a: number;
    b?: number;
}
export function decide(children: Child[], cap: number, now: number, split?: number, merge?: number, hysteresis = 60000): Decision | undefined {
    if (merge === undefined || now - merge >= hysteresis)
        for (let a = 0; a < children.length; a++) {
            const c = children[a];
            if (c.sockets !== undefined && c.sockets * 100 >= cap * 80 && (c.hi ?? (1n << 64n)) - c.lo > 1n)
                return { kind: 'split', a };
        }
    if (split !== undefined && now - split < hysteresis)
        return;
    for (let a = 0; a < children.length; a++) {
        const c = children[a];
        if (c.sockets === undefined || c.sockets * 100 >= cap * 30)
            continue;
        const neighbors = [a - 1, a + 1].filter(i => i >= 0 && i < children.length && children[i].sockets !== undefined)
            .sort((i, j) => children[i].sockets! - children[j].sockets!);
        const b = neighbors[0];
        if (b !== undefined && (c.sockets + children[b].sockets!) * 100 <= cap * 60)
            return { kind: 'merge', a, b };
    }
}
export class Suspect {
    failures = 0;
    due = 0;
    fail(now: number) { this.due = now + 2000 * 2 ** Math.min(this.failures++, 3); }
    success() { this.failures = 0; this.due = 0; }
}
export interface Relay {
    name: string;
    edges: string[];
}
export interface Operation {
    relays?: Relay[];
    id: string;
    kind: 'boot' | 'split' | 'merge';
    step: number;
    a: Child;
    b: Child;
    expect: bigint;
    started: number;
}
export interface ScaleState {
    relays?: Relay[];
    refused?: number;
    children: Child[];
    next: number;
    version: bigint;
    split?: number;
    merge?: number;
    op?: Operation;
}
export type Command = {
    kind: 'relay-init' | 'relay-end';
    name: string;
} | {
    kind: 'init' | 'range' | 'drain';
    child: Child;
} | {
    kind: 'end';
    child: Child;
    force: boolean;
} | {
    kind: 'create';
    child: Child;
} | {
    kind: 'split' | 'merge';
    op: Operation;
};
export interface ScaleIO {
    load(): ScaleState | undefined;
    /** Atomically persist metadata and the operation row, durably. */
    save(state: ScaleState): void;
    execute(command: Command): void;
}
export class Scaling {
    constructor(readonly io: ScaleIO, readonly prefix: string, readonly capacity = 500, readonly relayPrefix = prefix, readonly hysteresis = 60000) {
        if (!Number.isInteger(capacity) || capacity < 1 || capacity > 500)
            throw Error('invalid edge capacity');
        if (!Number.isSafeInteger(hysteresis) || hysteresis < 0)
            throw Error('invalid hysteresis');
    }
    state(): ScaleState { return this.io.load() ?? { children: [], next: 0, version: 0n }; }
    /** One durable step. A failed reply or save repeats the same command. */
    step(s: ScaleState, now: number): void {
        const o = s.op!;
        if (o.kind === 'boot') {
            if (o.step === 0) {
                this.io.execute({ kind: 'init', child: o.a });
                s.children = [{ ...o.a }];
            }
            else {
                this.io.execute({ kind: 'create', child: o.a });
                s.version = 1n;
                s.op = undefined;
            }
        }
        else if (o.kind === 'split') {
            if (o.step === 0) {
                for (const r of o.relays ?? [])
                    if (!(s.relays ?? []).some(old => old.name === r.name))
                        this.io.execute({ kind: 'relay-init', name: r.name });
                // Upgrade R4 configurations before relays become active.
                if (!(s.relays ?? []).length && (o.relays ?? []).length)
                    for (const child of s.children)
                        this.io.execute({ kind: 'range', child });
                this.io.execute({ kind: 'init', child: o.b });
                s.relays = o.relays;
                if (!s.children.some(c => c.name === o.b.name))
                    s.children.push({ ...o.b });
            }
            else if (o.step === 1) {
                this.io.execute({ kind: 'split', op: o });
                s.version = o.expect + 1n;
            }
            else {
                const a = { ...o.a, hi: o.b.lo };
                this.io.execute({ kind: 'range', child: a });
                s.children = s.children.map(c => c.name === a.name ? a : c);
                s.split = now;
                s.op = undefined;
            }
        }
        else {
            const union = { ...o.b, lo: o.a.lo < o.b.lo ? o.a.lo : o.b.lo, hi: o.a.hi === undefined || o.b.hi === undefined ? undefined : (o.a.hi > o.b.hi ? o.a.hi : o.b.hi) };
            if (o.step === 0) {
                this.io.execute({ kind: 'range', child: union });
                s.children = s.children.map(c => c.name === union.name ? union : c);
            }
            else if (o.step === 1) {
                this.io.execute({ kind: 'merge', op: o });
                s.version = o.expect + 1n;
            }
            else if (o.step === 2)
                this.io.execute({ kind: 'drain', child: o.a });
            else {
                this.io.execute({ kind: 'end', child: o.a, force: now - o.started >= 30000 });
                for (const r of s.relays ?? [])
                    if (!(o.relays ?? []).some(next => next.name === r.name))
                        this.io.execute({ kind: 'relay-end', name: r.name });
                s.relays = o.relays;
                s.children = s.children.filter(c => c.name !== o.a.name);
                s.merge = now;
                s.op = undefined;
            }
        }
        // Every touched count requires a subsequent push; never use init/range counts for another decision.
        s.children = s.children.map(c => ({ ...c, sockets: undefined }));
        s.children.sort((a, b) => a.lo < b.lo ? -1 : a.lo > b.lo ? 1 : 0);
        if (s.op)
            s.op.step++;
        this.io.save(s);
    }
    begin(s: ScaleState, now: number): boolean {
        if (s.op)
            return true;
        const d = decide(s.children, this.capacity, now, s.split, s.merge, this.hysteresis);
        if (d?.kind === 'split' && s.children.length >= 256) {
            s.refused = (s.refused ?? 0) + 1;
            this.io.save(s);
            throw Error('room edge ceiling: 256');
        }
        if (s.children.length && !d)
            return false;
        if (!Number.isSafeInteger(s.next) || s.next > Number.MAX_SAFE_INTEGER - 2)
            throw Error('scale child counter exhausted');
        const id = `${this.prefix}.op${s.next++}`;
        const a: Child = s.children.length ? { ...s.children[d!.a] } : { name: `${this.prefix}.e${s.next++}`, lo: 0n };
        const b: Child = d?.kind === 'split' ? { name: `${this.prefix}.e${s.next++}`, lo: a.lo + ((a.hi ?? (1n << 64n)) - a.lo) / 2n, hi: a.hi } : d ? { ...s.children[d.b!] } : a;
        a.sockets = undefined;
        b.sockets = undefined;
        let relays = structuredRelays(s.relays ?? []);
        const names = d?.kind === 'split' ? [...s.children.map(c => c.name), b.name] : d?.kind === 'merge' ? s.children.filter(c => c.name !== a.name).map(c => c.name) : [a.name];
        if (names.length <= 12)
            relays = [];
        else if (names.length > 16 || relays.length) {
            relays = relays.map(r => ({ ...r, edges: r.edges.filter(n => names.includes(n)) })).filter(r => r.edges.length);
            for (const name of names) {
                if (relays.some(r => r.edges.includes(name)))
                    continue;
                let r = [...relays].sort((a, b) => a.edges.length - b.edges.length)[0];
                if (!r || r.edges.length === 16) {
                    r = { name: `${this.relayPrefix}.r${s.next++}`, edges: [] };
                    relays.push(r);
                }
                r.edges.push(name);
            }
        }
        s.op = { relays, id, kind: d?.kind ?? 'boot', step: 0, a, b, expect: s.version, started: now };
        this.io.save(s);
        return true;
    }
}
function structuredRelays(rs: Relay[]): Relay[] { return rs.map(r => ({ ...r, edges: [...r.edges] })); }
