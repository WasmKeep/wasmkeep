import * as alarms from "./room-alarms-stub";
import assert from "node:assert/strict";
import { Round } from "../src/broadcast-round";
import { Core } from "../src/broadcast-core";
import * as db from "./directory-storage-stub";
import { modes, sources } from "./room-source";
for (const mode of modes) {
const { test, install, of } = sources(mode);
test("rounds retry suspects, recover positions and preserve R2 epochs", () => {
    db.install(":memory:");
    let core = new Core(db);
    core.init();
    core.publish([{ body: "old" }], 0n);
    const requests: any[] = [];
    let fail = false;
    install((_a, r) => { requests.push(JSON.parse(new TextDecoder().decode(r.body))); return { status: fail ? 503 : 200, headers: [], body: new TextEncoder().encode(r.path.endsWith("init") ? '{}' : JSON.stringify({ e: 1, n: 0, lagging: 0 })) }; });
    const round = new Round(["edge/e"]);
    round.run(of(core), 0);
    round.run(of(core), 20);
    fail = true;
    round.run(of(core), 40);
    assert.equal(alarms.pending, 2040n);
    const count = requests.length;
    round.run(of(core), 100);
    assert.equal(requests.length, count);
    fail = false;
    core = new Core(db);
    core.publish([{ body: "new" }], 100n);
    round.run(of(core), 2040);
    const p = requests.at(-1);
    assert.deepEqual(p.messages.map((m: any) => [m.e, m.b, m.d]), [[1, 0, "old"], [2, 1, "new"]]);
    assert.throws(() => new Round(Array(17).fill("edge/e")));
    db.close();
});
test("round refuses unsafe JSON numbers instead of rounding R2 integers", () => {
    db.install(":memory:");
    const core = new Core(db);
    core.init();
    core.publish([{ body: "x" }], 0n);
    db.exec("UPDATE wasmkeep_room_history SET n=?1", [{ tag: "integer", val: 9007199254740993n }]);
    install(() => ({ status: 200, headers: [], body: new TextEncoder().encode('{}') }));
    const round = new Round(["edge/e"]);
    round.run(of(core), 0);
    assert.throws(() => round.run(of(core), 20));
    db.close();
});
test("old epoch acknowledgment above the current head rolls back instead of becoming suspect", () => {
    db.install(":memory:");
    let core = new Core(db);
    core.init();
    core.publish([{ body: "kept" }, { body: "lost" }], 0n);
    const requests: any[] = [];
    install((_a, r) => { requests.push(JSON.parse(new TextDecoder().decode(r.body))); return { status: 200, headers: [], body: new TextEncoder().encode(r.path.endsWith("init") ? '{}' : JSON.stringify({ e: 1, n: 2, lagging: 0 })) }; });
    db.exec("DELETE FROM wasmkeep_room_history WHERE n>1", []);
    core = new Core(db);
    const round = new Round(["edge/e"]);
    round.run(of(core), 0);
    round.run(of(core), 20);
    assert.equal(alarms.pending, 40n);
    core.publish([{ body: "replacement" }], 21n);
    round.run(of(core), 40);
    assert.equal(requests.at(-1).messages[0].d, "replacement");
    assert.equal(requests.at(-1).b, 1);
    db.close();
});
test("a header at the largest safe head does not overflow its unused first",()=>{
 db.install(":memory:");const core=new Core(db);core.init();core.publish([{body:"x"}],0n);
 db.exec("UPDATE wasmkeep_room_history SET n=?1",[{tag:"integer",val:9007199254740991n}]);
 install(()=>({status:200,headers:[],body:new TextEncoder().encode(JSON.stringify({e:1,n:9007199254740991,lagging:0}))}));
 const round=new Round(["edge/e"]);round.run(of(core), 0);assert.doesNotThrow(()=>round.run(of(core), 20));db.close();
});
test("expired epoch metadata uses HTTP recovery rather than inventing a base", () => {
    db.install(":memory:"); let core = new Core(db); core.init(); core.publish([{ body: "old" }], 0n);
    core = new Core(db); core.wake();
    db.exec("DELETE FROM wasmkeep_room_epochs WHERE e=1", []);
    const requests: any[] = [];
    install((_a, r) => { requests.push(JSON.parse(new TextDecoder().decode(r.body))); return { status: 200, headers: [], body: new TextEncoder().encode(r.path.endsWith("init") ? '{}' : JSON.stringify({ e: 1, n: 0, lagging: 0 })) }; });
    const round = new Round(["edge/e"]); round.run(of(core), 0); round.run(of(core), 20); round.run(of(core), 40);
    assert.deepEqual(requests.at(-1).messages, []);
    assert.equal(requests.at(-1).recover, true); db.close();
});
test("stalled-only lag backs off alarm writes, new messages reset the delay", () => {
    db.install(":memory:");
    const core = new Core(db); core.init(); core.publish([{body:'one'}],0n);
    let head = 1, lagging = 1;
    install((_a,r) => ({status:200,headers:[],body:new TextEncoder().encode(r.path.endsWith('init')?'{}':JSON.stringify({e:1,n:head,lagging,stalled:lagging}))}));
    const round = new Round(['edge/e']); round.run(of(core), 0);
    const beforeWrites = alarms.writes;
    let now = 20;
    const delays: number[] = [];
    for(let i=0;i<15;i++) { round.run(of(core), now); const delay=Number(alarms.pending)-now; delays.push(delay); now+=delay; }
    assert.equal(alarms.writes - beforeWrites, 15);
    assert(now > 60000); // formerly 15 alarm writes in just 300 ms
    assert.deepEqual(delays.slice(0,5), [20,40,80,160,320]);
    assert.equal(delays.at(-1), 45000);
    head=2; core.publish([{body:'healthy'}],BigInt(now)); round.run(of(core), now);
    assert.equal(alarms.pending,BigInt(now+20));
    lagging=0; round.run(of(core), now+20); db.close();
});
}
