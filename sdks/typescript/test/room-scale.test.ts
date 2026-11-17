import { test } from 'node:test';
import assert from 'node:assert/strict';
import { decide, Suspect, type Child } from '../src/room-scale';
import { Edge } from '../src/room-edge';
const child = (lo: bigint, hi: bigint | undefined, sockets: number): Child => ({ name: `edge/${lo}`, lo, hi, sockets });
test('R4 thresholds, adjacent pair rule and minute hysteresis', () => {
    const a = child(0n, 1n << 63n, 8), b = child(1n << 63n, undefined, 2);
    assert.equal(decide([a, b], 10, 60000, undefined, 1)?.kind, undefined);
    assert.equal(decide([a, b], 10, 60001, undefined, 1)?.kind, 'split');
    a.sockets = 4;
    assert.equal(decide([a, b], 10, 60000, 1, undefined), undefined);
    assert.equal(decide([a, b], 10, 60001, 1, undefined)?.kind, 'merge');
    b.sockets = 3;
    assert.equal(decide([a, b], 10, 60001), undefined);
    b.sockets = 2;
    a.sockets = 5;
    assert.equal(decide([a, b], 10, 60001), undefined);
    a.sockets = undefined;
    assert.equal(decide([a, b], 10, 60001), undefined);
});
test('R4 suspects skip 2,4,8,16 seconds and clear on success', () => {
    const s = new Suspect();
    let now = 0;
    for (const delay of [2000, 4000, 8000, 16000, 16000]) {
        s.fail(now);
        assert.equal(s.due, now + delay);
        now = s.due;
    }
    s.success();
    s.fail(now);
    assert.equal(s.due, now + 2000);
});
test('R4 split closes upper half with 4801 and resumes by number; merge drains', () => {
    const closed: [
        bigint,
        number
    ][] = [];
    const frames: string[] = [];
    const io = { send: (_id: bigint, s: string) => { frames.push(s); return 'ok' as const; }, close: (id: bigint, c: number) => { closed.push([id, c]); } };
    const a = new Edge(io), b = new Edge(io);
    a.push({ e: 1, b: 0, first: 1, head: 0, messages: [] });
    for (let i = 0; i < 4; i++) {
        a.open(BigInt(i), undefined);
        a.setPoint(BigInt(i), BigInt(i) * (1n << 62n));
    }
    a.push({ e: 1, b: 0, first: 1, head: 1, messages: [{ a: 0, d: 'one' }] });
    a.range(0n, 1n << 63n, false, [0n, 1n, 2n, 3n]);
    assert.deepEqual(closed, [[2n, 4801], [3n, 4801]]);
    b.push({ e: 1, b: 0, first: 1, head: 2, messages: [{ a: 0, d: 'one' }, { a: 0, d: 'two' }] });
    b.open(2n, { e: 1, n: 1 });
    assert.equal(JSON.parse(frames.at(-1)!).n, 2);
    b.range(0n, undefined, true, [2n]);
    assert.deepEqual(closed.at(-1), [2n, 4801]);
});
import { Scaling, type ScaleState, type Command } from '../src/room-scale';
import { create, change, type State as DirectoryState } from '../src/directory-state';
test('R4 every split and merge step replays after an effect succeeds but its checkpoint is lost', () => {
    for (const kind of ['split', 'merge'] as const)
        for (let cut = 0; cut < (kind === 'split' ? 3 : 4); cut++) {
            let saved: ScaleState = { children: kind === 'split' ? [child(0n, undefined, 8)] : [child(0n, 1n << 63n, 1), child(1n << 63n, undefined, 2)], next: 3, version: kind === 'split' ? 1n : 2n };
            let directory: DirectoryState = create('room', saved.children[0].name, new Uint8Array());
            if (kind === 'merge')
                directory = change(directory, 'room', { kind: 'split', lo: 0n, at: 1n << 63n, target: saved.children[1].name, meta: new Uint8Array(), expect: 1n, op: 'setup' });
            const edges = new Map(saved.children.map(c => [c.name, { ...c }]));
            let fail = false;
            let count = 0;
            const io = { load: () => structuredClone(saved), save: (s: ScaleState) => {
                    if (fail) {
                        fail = false;
                        throw Error('crash');
                    }
                    saved = structuredClone(s);
                }, execute: (c: Command) => {
                    count++;
                    if (c.kind === 'split' || c.kind === 'merge') {
                        const o = c.op;
                        directory = change(directory, 'room', c.kind === 'split' ? { kind: 'split', lo: o.a.lo, at: o.b.lo, target: o.b.name, meta: new Uint8Array(), expect: o.expect, op: o.id } : { kind: 'merge', lo: o.a.lo > o.b.lo ? o.a.lo : o.b.lo, keep: o.b.lo < o.a.lo ? 'left' : 'right', expect: o.expect, op: o.id });
                    }
                    else if (c.kind === 'end')
                        edges.delete(c.child.name);
                    else if (c.kind === 'init' || c.kind === 'range')
                        edges.set(c.child.name, { ...c.child });
                } };
            let scale = new Scaling(io, 'edge/room', 10);
            let s = scale.state();
            assert.equal(scale.begin(s, 60000), true);
            for (let i = 0; i < cut; i++)
                scale.step(s, 60000);
            fail = true;
            assert.throws(() => scale.step(s, 60000), /crash/);
            scale = new Scaling(io, 'edge/room', 10);
            s = scale.state();
            while (s.op)
                scale.step(s, 60000);
            assert.equal(directory.map.entries.length, kind === 'split' ? 2 : 1);
            assert.equal(s.children.length, directory.map.entries.length);
            assert.equal(edges.size, s.children.length);
            assert.equal(count, (kind === 'split' ? 3 : 4) + 1);
            assert(s.children.every(c => c.sockets === undefined));
        }
});
import { ScaleHost } from '../src/broadcast-scaling';
import * as db from './directory-storage-stub';
test('R4 durable metadata and operation row roll back together on every checkpoint failure', () => {
    for (const kind of ['split', 'merge'] as const)
        for (let cut = 0; cut < (kind === 'split' ? 3 : 4); cut++) {
            db.install(':memory:');
            let host = new ScaleHost({ directory: 'directory/bg:r', edgeClass: 'edge', capacity: 10 });
            host.init();
            const initial: ScaleState = { children: kind === 'split' ? [child(0n, undefined, 8)] : [child(0n, 1n << 63n, 1), child(1n << 63n, undefined, 2)], next: 3, version: kind === 'split' ? 1n : 2n };
            let effects = 0;
            const io = { load: () => host.load(), save: (s: ScaleState) => host.save(s), execute: (_c: Command) => { effects++; } };
            let scale = new Scaling(io, 'edge/r', 10);
            let s = initial;
            scale.begin(s, 60000);
            for (let i = 0; i < cut; i++)
                scale.step(s, 60000);
            db.failNextCommit();
            assert.throws(() => scale.step(s, 60000), /commit/);
            host = new ScaleHost({ directory: 'directory/bg:r', edgeClass: 'edge', capacity: 10 });
            scale = new Scaling(io, 'edge/r', 10);
            s = scale.state();
            assert.equal(s.op!.step, cut);
            while (s.op)
                scale.step(s, 60000);
            assert.equal(effects, (kind === 'split' ? 3 : 4) + 1);
            assert.equal(db.query('SELECT * FROM wasmkeep_room_ops', []).values.length, 0);
            assert.equal(host.load()!.children.length, kind === 'split' ? 2 : 1);
            db.close();
        }
});
import { Core } from '../src/broadcast-core';
import { Round } from '../src/broadcast-round';
import * as alarms from './room-alarms-stub';
import { install } from './host-stub';
test('a blocked scale operation never prevents rounds to either side of its map change', () => {
    db.install(':memory:');
    const host = new ScaleHost({ directory: 'directory/bg:r', edgeClass: 'edge', capacity: 10 });
    host.init();
    const a = child(0n, undefined, 8), b = child(1n << 63n, undefined, 0);
    host.save({ children: [a, b], next: 3, version: 1n, op: { id: 'split', kind: 'split', step: 1, a, b, expect: 1n, started: 0 } });
    const core = new Core(db, alarms);
    core.init();
    let pushes = 0;
    install((address, r) => {
        if (address.startsWith('directory/'))
            throw Error('directory unavailable');
        assert(r.path.endsWith('/push'));
        pushes++;
        return { status: 200, headers: [], body: new TextEncoder().encode('{"e":1,"n":0,"sockets":0,"lagging":0}') };
    });
    assert.throws(() => host.run(new Round([]), core, 10), /unavailable/);
    assert.equal(pushes, 2);
    assert.equal(host.load()!.op!.step, 1);
    db.close();
});
test('R5 grows past sixteen direct edges', () => {
    const edges = Array.from({ length: 16 }, (_, i) => child(BigInt(i) * (1n << 60n), i === 15 ? undefined : BigInt(i + 1) * (1n << 60n), 8));
    assert.equal(decide(edges, 10, 60000)?.kind, 'split');
});
test('R5 topology creation/retirement survives every lost durable checkpoint; 256 refuses', () => {
    for (const mode of ['grow', 'shrink'] as const)
        for (let cut = 0; cut < (mode === 'grow' ? 3 : 4); cut++) {
            const children = Array.from({ length: mode === 'grow' ? 16 : 13 }, (_, i) => child(BigInt(i) * (1n << 59n), i === (mode === 'grow' ? 15 : 12) ? undefined : BigInt(i + 1) * (1n << 59n), mode === 'grow' ? 8 : 1));
            let saved: ScaleState = { children, next: 100, version: 1n, relays: mode === 'grow' ? [] : [{ name: 'relay/r.r90', edges: children.map(c => c.name) }] };
            const active = new Set((saved.relays ?? []).map(r => r.name));
            let fail = false;
            const io = { load: () => structuredClone(saved), save: (s: ScaleState) => { if (fail) {
                    fail = false;
                    throw Error('crash');
                } saved = structuredClone(s); }, execute: (c: Command) => { if (c.kind === 'relay-init')
                    active.add(c.name); if (c.kind === 'relay-end')
                    active.delete(c.name); } };
            let scale = new Scaling(io, 'edge/r', 10, 'relay/r');
            let s = scale.state();
            scale.begin(s, 60000);
            for (let i = 0; i < cut; i++)
                scale.step(s, 60000);
            fail = true;
            assert.throws(() => scale.step(s, 60000), /crash/);
            scale = new Scaling(io, 'edge/r', 10, 'relay/r');
            s = scale.state();
            while (s.op)
                scale.step(s, 60000);
            assert.equal(s.children.length, mode === 'grow' ? 17 : 12);
            assert.equal(s.relays?.length, mode === 'grow' ? 2 : 0);
            assert.deepEqual([...active].sort(), (s.relays ?? []).map(r => r.name).sort());
            assert((s.relays ?? []).every(r => r.edges.length <= 16));
            if (mode === 'grow')
                assert.equal(new Set(s.relays!.flatMap(r => r.edges)).size, 17);
        }
    const children = Array.from({ length: 256 }, (_, i) => child(BigInt(i) * (1n << 56n), i === 255 ? undefined : BigInt(i + 1) * (1n << 56n), 8));
    const s: ScaleState = { children, next: 300, version: 1n };
    const scale = new Scaling({ load: () => s, save: () => { }, execute: () => assert(false, 'no effect at ceiling') }, 'edge/r', 10);
    assert.throws(() => scale.begin(s, 60000), /ceiling: 256/);
    assert.equal(s.refused, 1);
    assert.equal(s.op, undefined);
    assert.equal(decide(children.slice(0, 1), 10, 10, undefined, 0, 10)?.kind, 'split');
});
