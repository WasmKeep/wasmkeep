import assert from 'node:assert/strict';
import { packRelay, unpackRelay } from '../src/room-relay';
import { Edge, type Push } from '../src/room-edge';
import { Round } from '../src/broadcast-round';
import { Core } from '../src/broadcast-core';
import * as db from './directory-storage-stub';
import * as alarms from './room-alarms-stub';
import {defineRelay} from '../src/broadcast-relay';
import type {ActorRequest} from '../src/request';
import { install as hostInstall } from './host-stub';
import { modes, sources } from './room-source';
for (const mode of modes) {
const { test, install, of } = sources(mode);
test('R5 relay shares one slice, preserves each child order, and bounds fanout', () => {
    const push: Push = { e: 1, b: 0, first: 1, head: 3, messages: [1, 2, 3].map(n => ({ a: n, d: `message${n}` })) };
    const packed = packRelay([{ edge: 'edge/a', push }, { edge: 'edge/b', push: { ...push, first: 2, messages: push.messages.slice(1) } }]);
    assert.equal(JSON.stringify(packed).split('message1').length, 2);
    const received: number[][] = [[], []];
    unpackRelay(packed).forEach((c, i) => {
        const edge = new Edge({ send: (_id, s) => { const f = JSON.parse(s); if (f.t === 'm')
                f.m.forEach((_: unknown, j: number) => received[i].push(f.n + j)); return 'ok'; }, close: () => { } });
        edge.push({ ...push, head: i, first: i + 1, messages: [] });
        edge.open(1n, { e: 1, n: i });
        edge.push(c.push);
        edge.push(c.push);
    });
    assert.deepEqual(received, [[1, 2, 3], [2, 3]]);
    assert.throws(() => packRelay(Array.from({ length: 17 }, () => ({ edge: 'edge/a', push }))), /fan-out/);
});
test('R5 a failed relay is skipped, then retried without poisoning edges', () => {
    db.install(':memory:');
    const core = new Core(db);
    core.init();
    const round = new Round([]);
    round.sync([{ name: 'edge/a' }], [{ name: 'relay/r.r1', edges: ['edge/a'] }]);
    let calls = 0;
    install((address, req) => { assert.equal(address, 'relay/r.r1'); assert(req.path.includes('relay')); calls++; if (calls === 1)
        throw { payload: { tag: 'timeout' } }; return { status: 200, headers: [], body: new TextEncoder().encode('[{"e":1,"n":0,"lagging":0,"sockets":2}]') }; });
    round.run(of(core), 0);
    assert.equal(calls, 1);
    assert.equal(round.count('edge/a'), undefined);
    round.run(of(core), 1999);
    assert.equal(calls, 1);
    round.run(of(core), 2000);
    assert.equal(calls, 2);
    assert.equal(round.count('edge/a'), 2);
    round.run(of(core), 2001);
    assert.equal(calls, 3);
    db.close();
});
test('R5 an edge failure skips only that edge inside a healthy relay', () => {
    db.install(':memory:');
    const core = new Core(db);
    core.init();
    const round = new Round([]);
    round.sync([{ name: 'edge/a' }, { name: 'edge/b' }], [{ name: 'relay/r.r1', edges: ['edge/a', 'edge/b'] }]);
    const sent: string[][] = [];
    install((_address, req) => {
        const names = unpackRelay(JSON.parse(new TextDecoder().decode(req.body))).map(c => c.edge);
        sent.push(names);
        return { status: 200, headers: [], body: new TextEncoder().encode(JSON.stringify(names.map(n => n === 'edge/a' && sent.length === 1 ? null : { e: 1, n: 0, sockets: 1, lagging: 0 }))) };
    });
    round.run(of(core), 0);
    round.run(of(core), 20);
    round.run(of(core), 2000);
    assert.deepEqual(sent, [['edge/a', 'edge/b'], ['edge/b'], ['edge/a', 'edge/b']]);
    db.close();
});
test('R5 relay handler makes one downward batch and returns ordered edge answers',()=>{
    const relay=defineRelay();
    const push:Push={e:1,b:0,first:1,head:0,messages:[]};
    const payload=JSON.stringify(packRelay([{edge:'edge/a',push},{edge:'edge/b',push}]));
    const seen:string[]=[];
    install((address,r)=>{
        seen.push(address);assert.equal(r.path,'/wasmkeep-edge/v1/push');
        if(address==='edge/b')throw {payload:{tag:'timeout'}};
        return {status:200,headers:[],body:new TextEncoder().encode('{"e":1,"n":0,"sockets":1,"lagging":0}')};
    });
    const request:ActorRequest={method:'POST',path:'/wasmkeep-relay/v1/push',headers:{'x-wasmkeep-caller':'tenant/room/r'},text:()=>payload,bytes:()=>new TextEncoder().encode(payload),json:<T>()=>JSON.parse(payload) as T};
    const reply=relay.fetch!(request);assert(typeof reply==='object' && !(reply instanceof Uint8Array));
    assert.equal(reply.status,200);assert.equal(typeof reply.body,'string');
    assert.deepEqual(JSON.parse(reply.body as string),[{e:1,n:0,sockets:1,lagging:0},null]);
    assert.deepEqual(seen,['edge/a','edge/b']);
});
}
