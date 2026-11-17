import { test } from 'node:test';
import assert from 'node:assert/strict';
import { join } from '../src/broadcast-client';
import { define } from '../src/broadcast';
import { Writer } from '../src/wire';
import { install } from './host-stub';
import * as db from './directory-storage-stub';
import * as alarms from './room-alarms-stub';
test('join batches lookup and poke, ignores poke failure, retries missing map once', () => {
    const requests: string[] = [];
    let lookups = 0;
    install((a, r) => {
        requests.push(a + r.path);
        if (a === 'room/r')
            return { status: 503, headers: [], body: new Uint8Array() };
        if (!lookups++)
            return { status: 404, headers: [], body: new Uint8Array() };
        return { status: 200, headers: [], body: new Writer().u64(1).u64(0).u64(0).u64(0).bytes(new TextEncoder().encode('edge/r.e1')).u64(1).bytes(new Uint8Array()).finish() };
    });
    const r = join('room/r', 'directory/bg:r', 'client&x', { e: 1n, n: 7n });
    assert(typeof r === 'object' && !(r instanceof Uint8Array));
    assert.equal(r.status, 426);
    assert.equal(requests.length, 3);
    assert(requests[1].endsWith('/poke'));
    assert(JSON.stringify(r).includes('&e=1&n=7'));
    assert(JSON.stringify(r).includes('client%26x'));
});
test('poke schedules an alarm without doing directory, push or scaling calls', () => {
    db.install(':memory:');
    alarms.clearAlarm();
    let calls = 0;
    install(() => { calls++; throw Error('poke must not call'); });
    const room = define({ scaling: { directory: 'directory/bg:r', edgeClass: 'edge', capacity: 5 } });
    room.init!();
    const r = room.fetch!({ method: 'POST', path: '/wasmkeep-room/v1/poke', headers: { 'x-wasmkeep-caller': 'tenant/_front/main' }, bytes: () => new Uint8Array(), text: () => '', json: <T>() => ({}) as T });
    assert(typeof r === 'object' && !(r instanceof Uint8Array));
    assert.equal(r.status, 200);
    assert.equal(calls, 0);
    assert(alarms.pending !== undefined);
    db.close();
});

test('QuickJS null: poke schedules an alarm without doing directory, push or scaling calls', () => {
    db.install(':memory:');
    alarms.clearAlarm();
    alarms.useQuickJS(true);
    let calls = 0;
    install(() => { calls++; throw Error('poke must not call'); });
    const room = define({ scaling: { directory: 'directory/bg:r', edgeClass: 'edge', capacity: 5 } });
    room.init!();
    const r = room.fetch!({ method: 'POST', path: '/wasmkeep-room/v1/poke', headers: { 'x-wasmkeep-caller': 'tenant/_front/main' }, bytes: () => new Uint8Array(), text: () => '', json: <T>() => ({}) as T });
    assert(typeof r === 'object' && !(r instanceof Uint8Array));
    assert.equal(r.status, 200);
    assert.equal(calls, 0);
    assert(alarms.pending !== undefined);
    alarms.useQuickJS(false);
    db.close();
});
