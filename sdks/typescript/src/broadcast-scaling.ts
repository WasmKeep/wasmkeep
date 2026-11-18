import * as storage from 'wasmkeep:actor/storage@0.2.0';
import * as alarms from 'wasmkeep:actor/alarms@0.2.0';
import * as actors from 'wasmkeep:actor/actors@0.2.0';
import { Client as Directory } from './directory-client';
import { Scaling, decide, type ScaleState, type Command, type Child } from './room-scale';
import type { Core } from './broadcast-core';
import type { Round } from './broadcast-round';
const encode = (s: unknown) => JSON.stringify(s, (_k, v) => typeof v === 'bigint' ? { $u64: v.toString() } : v);
const decode = (s: string) => JSON.parse(s, (_k, v) => v && typeof v === 'object' && '$u64' in v ? BigInt(v.$u64) : v);
export interface ScaleOptions {
    directory: string;
    edgeClass: string;
    capacity?: number;
    relayClass?: string;
    hysteresisMs?: number;
}
export class ScaleHost {
    private scaling: Scaling;
    private head = { e: 0, n: 0 };
    private room: string;
    constructor(private options: ScaleOptions) {
        const a = actors.selfAddress();
        this.room = `${a.tenant}/${a.class}/${a.name}`;
        this.scaling = new Scaling(this, `${options.edgeClass}/${a.name}`, options.capacity ?? 500, `${options.relayClass ?? "relay"}/${a.name}`, options.hysteresisMs ?? 60000);
    }
    init() {
        storage.exec('CREATE TABLE IF NOT EXISTS wasmkeep_room_scale(id INTEGER PRIMARY KEY CHECK(id=1),data TEXT NOT NULL)', []);
        storage.exec('CREATE TABLE IF NOT EXISTS wasmkeep_room_ops(id TEXT PRIMARY KEY,kind TEXT NOT NULL,step INTEGER NOT NULL,args TEXT NOT NULL)', []);
    }
    load(): ScaleState | undefined {
        const row = storage.query('SELECT data FROM wasmkeep_room_scale WHERE id=1', []).values[0]?.[0];
        if (row?.tag !== 'text')
            return;
        const state = decode(row.val) as ScaleState;
        const op = storage.query('SELECT args FROM wasmkeep_room_ops', []).values[0]?.[0];
        state.op = op?.tag === 'text' ? decode(op.val) : undefined;
        return state;
    }
    save(state: ScaleState) {
        storage.exec('BEGIN', []);
        try {
            storage.exec('INSERT INTO wasmkeep_room_scale(id,data) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET data=excluded.data', [{ tag: 'text', val: encode({ ...state, op: undefined, children: state.children.map(c => ({ ...c, sockets: undefined })) }) }]);
            storage.exec('DELETE FROM wasmkeep_room_ops', []);
            if (state.op)
                storage.exec('INSERT INTO wasmkeep_room_ops(id,kind,step,args) VALUES(?1,?2,?3,?4)', [{ tag: 'text', val: state.op.id }, { tag: 'text', val: state.op.kind }, { tag: 'integer', val: BigInt(state.op.step) }, { tag: 'text', val: encode(state.op) }]);
            storage.execDurable('COMMIT', []);
        }
        catch (e) {
            storage.exec('ROLLBACK', []);
            throw e;
        }
    }
    execute(c: Command) {
        if (c.kind === 'relay-init' || c.kind === 'relay-end') {
            const r = actors.call(c.name, { method: 'POST', path: `/wasmkeep-relay/v1/${c.kind === 'relay-init' ? 'init' : 'end'}`, headers: [], body: new Uint8Array() });
            if (r.status !== 200)
                throw Error(`scale ${c.kind} answered ${r.status}`);
            return;
        }
        const dir = new Directory(this.options.directory);
        if (c.kind === 'create') {
            dir.create(c.child.name);
            return;
        }
        if (c.kind === 'split' || c.kind === 'merge') {
            const o = c.op;
            dir.change(c.kind === 'split' ? { kind: 'split', lo: o.a.lo, at: o.b.lo, target: o.b.name, meta: new Uint8Array(), expect: o.expect, op: o.id } : { kind: 'merge', lo: o.a.lo > o.b.lo ? o.a.lo : o.b.lo, keep: o.b.lo < o.a.lo ? 'left' : 'right', expect: o.expect, op: o.id });
            return;
        }
        if (!('child' in c))
            throw Error('invalid scale command');
        const child = c.child;
        const hex = (n: bigint) => n.toString(16).padStart(16, '0');
        const body = c.kind === 'end' ? (c.force ? 'force' : '') : JSON.stringify({ room: this.room, relayPrefix: `${this.room.split("/")[0]}/${this.options.relayClass ?? "relay"}/${this.room.split("/").slice(2).join("/")}.r`, lo: hex(child.lo), hi: child.hi === undefined ? undefined : hex(child.hi), state: c.kind === 'drain' ? 'draining' : 'active', cap: this.options.capacity ?? 500, ...this.head });
        const r = actors.call(child.name, { method: 'POST', path: `/wasmkeep-edge/v1/${c.kind}`, headers: [], body: new TextEncoder().encode(body) });
        if (r.status !== 200)
            throw Error(`scale ${c.kind} answered ${r.status}`);
    }
    /** One durable step of the pending operation, or of a new one the
     * counts call for: never more per event, so publishes, pokes and
     * history wait behind at most one step's calls and save (ADR 0040,
     * section 7). False when there is nothing to do. */
    private advance(state: ScaleState, now: number): boolean {
        // A step that fails (or the edge ceiling) leaves its operation for
        // the alarm 2 s later; with nothing to do the round's alarm stands.
        const before = alarms.getAlarm();
        alarms.setAlarm(BigInt(now + 2000));
        if (!this.scaling.begin(state, now)) {
            if (before == null)
                alarms.clearAlarm();
            else
                alarms.setAlarm(before);
            return false;
        }
        this.scaling.step(state, now);
        return true;
    }
    run(round: Round, core: Core, now: number) {
        this.init();
        const e = core.wake(), n = core.last() ?? e.base;
        if (e.e > 9007199254740991n || n > 9007199254740991n)
            throw Error('invalid browser integer');
        this.head = { e: Number(e.e), n: Number(n) };
        const state = this.scaling.state();
        let stepped = false;
        // A group without children boots first, so this round reaches its edge.
        if (!state.children.length)
            stepped = this.advance(state, now);
        round.sync(state.children, state.relays);
        round.run(core, now);
        for (const c of state.children)
            c.sockets = round.count(c.name);
        if (!stepped)
            stepped = this.advance(state, now);
        // The alarm the round set stands unless scaling has more to do.
        if (stepped)
            alarms.setAlarm(BigInt(state.op ? now : now + 20));
    }
}
