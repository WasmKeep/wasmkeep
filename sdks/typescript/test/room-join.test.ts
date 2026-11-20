import { test } from 'node:test';
import assert from 'node:assert/strict';
import { join } from '../src/broadcast-client';
import { define } from '../src/broadcast';
import { definePusher } from '../src/broadcast-pusher';
import { Writer } from '../src/wire';
import { install } from './host-stub';
import * as db from './directory-storage-stub';
import * as alarms from './room-alarms-stub';
test('join batches lookup and the pusher poke, ignores poke failure, retries missing map once', () => {
    const requests: string[] = [];
    let lookups = 0;
    install((a, r) => {
        requests.push(a + r.path);
        if (a === 'pusher/r')
            return { status: 503, headers: [], body: new Uint8Array() };
        if (!lookups++)
            return { status: 404, headers: [], body: new Uint8Array() };
        return { status: 200, headers: [], body: new Writer().u64(1).u64(0).u64(0).u64(0).bytes(new TextEncoder().encode('edge/r.e1')).u64(1).bytes(new Uint8Array()).finish() };
    });
    const r = join('room/r', 'directory/bg:r', 'client&x', { e: 1n, n: 7n });
    assert(typeof r === 'object' && !(r instanceof Uint8Array));
    assert.equal(r.status, 426);
    assert.equal(requests.length, 3);
    assert.equal(requests[1], 'pusher/r/wasmkeep-pusher/v1/poke');
    assert(JSON.stringify(r).includes('&e=1&n=7'));
    assert(JSON.stringify(r).includes('client%26x'));
});
const poke = (actor: ReturnType<typeof definePusher>, body: Uint8Array = new Uint8Array()) => {
    const r = actor.fetch!({ method: 'POST', path: '/wasmkeep-pusher/v1/poke', headers: { 'x-wasmkeep-caller': 'tenant/_front/main' }, bytes: () => body, text: () => '', json: <T>() => ({}) as T });
    assert(typeof r === 'object' && !(r instanceof Uint8Array));
    return r.status;
};
for (const quickJS of [false, true])
    test(`${quickJS ? 'QuickJS null: ' : ''}a pusher's poke schedules an alarm without any call`, () => {
        db.install(':memory:');
        alarms.clearAlarm();
        alarms.useQuickJS(quickJS);
        let calls = 0;
        install(() => { calls++; throw Error('poke must not call'); });
        const pusher = definePusher({ scaling: { directory: 'directory/bg:r', edgeClass: 'edge', capacity: 5 } });
        pusher.init!();
        assert.equal(poke(pusher), 200);
        assert.equal(calls, 0);
        assert(alarms.pending !== undefined);
        // A publish's poke brings a later alarm in to 20 ms from now.
        alarms.setAlarm(BigInt(Date.now() + 45000));
        assert.equal(poke(pusher, new Writer().u64(1).finish()), 200);
        assert(alarms.pending! <= BigInt(Date.now() + 20));
        assert.equal(poke(pusher, new Writer().u64(2).finish()), 400);
        assert.equal(calls, 0);
        alarms.useQuickJS(false);
        db.close();
    });
test('the room sets no alarm, still answers a 0.6.0 poke, and refuses the old options', () => {
    db.install(':memory:');
    alarms.clearAlarm();
    install(() => { throw Error('the room must not call'); });
    const room = define();
    room.init!();
    const r = room.fetch!({ method: 'POST', path: '/wasmkeep-room/v1/poke', headers: { 'x-wasmkeep-caller': 'tenant/_front/main' }, bytes: () => new Uint8Array(), text: () => '', json: <T>() => ({}) as T });
    assert(typeof r === 'object' && !(r instanceof Uint8Array) && r.status === 200);
    room.alarm!();
    assert.equal(alarms.pending, undefined);
    assert.throws(() => define({ scaling: { directory: 'd', edgeClass: 'edge' } } as never), /definePusher/);
    db.close();
});
