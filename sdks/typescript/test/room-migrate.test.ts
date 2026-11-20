// ADR 0040, section 13: a room deployed under 0.6.0 (its scale rows, the
// directory map's ownership and every edge's controller) moves to its
// pusher, one step per alarm, and a pending operation resumes there.
import { test } from "node:test";
import assert from "node:assert/strict";
import { define } from "../src/broadcast";
import { definePusher } from "../src/broadcast-pusher";
import { defineEdge } from "../src/broadcast-edge";
import { define as defineDirectory, Client as DirectoryClient } from "../src/directory";
import { ScaleHost } from "../src/broadcast-scaling";
import { Client } from "../src/broadcast-client";
import type { ActorDefinition, ActorRequest } from "../src/index";
import { toBytes, Writer } from "../src/wire";
import * as db from "./directory-storage-stub";
import * as alarms from "./room-alarms-stub";
import { install, setSelf, type Request } from "./host-stub";

const MID = 1n << 63n;
/** Several actors, each with its own database and address; a call carries
 * its caller's address, as the host's `x-wasmkeep-caller` does. */
class World {
    private actors = new Map<string, ActorDefinition>();
    private self = "tenant/_front/main";
    log: string[] = [];
    constructor() {
        install((address, r) => this.call(address, r));
    }
    add(address: string, def: ActorDefinition) {
        this.actors.set(address, def);
        this.as(address, () => def.init?.());
    }
    as<T>(address: string, f: () => T): T {
        const before = this.self, [cls, name] = address.split("/");
        this.self = `tenant/${address}`;
        db.use(address);
        setSelf({ tenant: "tenant", class: cls, name });
        try {
            return f();
        }
        finally {
            this.self = before;
            const [c, n] = before.split("/").slice(1);
            db.use(before.split("/").slice(1).join("/"));
            setSelf({ tenant: "tenant", class: c, name: n });
        }
    }
    call(address: string, r: Request) {
        this.log.push(address + r.path);
        const def = this.actors.get(address);
        if (!def)
            throw { payload: { tag: "invalid-address", val: address } };
        const caller = this.self;
        return this.as(address, () => {
            const req: ActorRequest = { method: r.method, path: r.path, headers: { "x-wasmkeep-caller": caller }, bytes: () => r.body, text: () => new TextDecoder().decode(r.body), json: <T>() => JSON.parse(new TextDecoder().decode(r.body)) as T };
            const out = def.fetch!(req);
            assert(typeof out === "object" && !(out instanceof Uint8Array));
            return { status: out.status ?? 200, headers: [], body: typeof out.body === "string" ? toBytes(out.body) : out.body ?? new Uint8Array() };
        });
    }
    /** The pusher's alarm, fired. */
    alarm(address: string) {
        // As a host before 0.7.0 does (the harder case the SDK still
        // supports): during on-alarm get-alarm still answers the alarm being
        // delivered, which is cleared after unless the handler moved it.
        // A delivered alarm is due: its time is now (a punctual delivery).
        const at = BigInt(Date.now());
        alarms.setAlarm(at);
        this.as(address, () => this.actors.get(address)!.alarm!());
        if (alarms.pending === at)
            alarms.clearAlarm();
    }
    query(address: string, sql: string) {
        return this.as(address, () => db.query(sql, []).values.map(r => r.map(v => (v as { val?: unknown }).val)));
    }
}
/** A 0.6.0 group: the room owns the map and both edges; `op` pending. */
function legacy(w: World, pending: boolean) {
    w.add("directory/bg:r", defineDirectory());
    for (const e of ["edge/r.e1", "edge/r.e2"])
        w.add(e, defineEdge());
    w.as("room/r", () => {
        const dir = new DirectoryClient("directory/bg:r");
        dir.create("edge/r.e1");
        dir.change({ kind: "split", lo: 0n, at: MID, target: "edge/r.e2", meta: new Uint8Array(), expect: 1n, op: "edge/r.op0" });
        const config = (lo: bigint, hi?: bigint) => toBytes(JSON.stringify({ room: "tenant/room/r", relayPrefix: "tenant/relay/r.r", lo: lo.toString(16).padStart(16, "0"), hi: hi?.toString(16).padStart(16, "0"), state: "active", cap: 5, e: 1, n: 0 }));
        // With the split's last step pending, e1 still has the whole space.
        assert.equal(w.call("edge/r.e1", { method: "POST", path: "/wasmkeep-edge/v1/init", headers: [], body: config(0n, pending ? undefined : MID) }).status, 200);
        assert.equal(w.call("edge/r.e2", { method: "POST", path: "/wasmkeep-edge/v1/init", headers: [], body: config(MID) }).status, 200);
        // The 0.6.0 room's rows, as its ScaleHost wrote them.
        const host = new ScaleHost({ directory: "directory/bg:r", edgeClass: "edge", capacity: 5 });
        host.init();
        const a = { name: "edge/r.e1", lo: 0n, hi: pending ? undefined : MID }, b = { name: "edge/r.e2", lo: MID };
        host.save({ children: [a, b], next: 3, version: 2n, split: Date.now(), op: pending ? { id: "edge/r.op0", kind: "split", step: 2, a: { name: "edge/r.e1", lo: 0n }, b, expect: 1n, started: 0, relays: [] } : undefined });
    });
}
function migrate(pending: boolean) {
    db.install(":memory:");
    alarms.clearAlarm();
    const w = new World();
    legacy(w, pending);
    w.add("room/r", define());
    w.add("pusher/r", definePusher({ scaling: { directory: "directory/bg:r", edgeClass: "edge", capacity: 5 } }));
    // Before the pusher asks, the map and the edges refuse it.
    w.as("pusher/r", () => assert.equal(w.call("edge/r.e2", { method: "POST", path: "/wasmkeep-edge/v1/range", headers: [], body: toBytes("{}") }).status, 403));
    // The first publish asks the front to poke the pusher, which wakes it.
    const pokes: string[] = [];
    w.add("probe/p", { fetch: () => ({ status: 200 }) });
    w.as("probe/p", () => new Client("room/r").publish([{ body: "after the upgrade" }]));
    assert(alarms.pending !== undefined, "the poke set the pusher's alarm");
    const steps: string[] = [];
    for (let i = 0; i < 12; i++) {
        w.alarm("pusher/r");
        steps.push(String(w.query("pusher/r", "SELECT step FROM wasmkeep_pusher_migration")[0]?.[0] ?? "none"));
    }
    // Read and adopt, hand over the map and both edges, retire: then rounds.
    assert.deepEqual(steps.slice(0, 3), ["1", "2", "-1"]);
    assert.equal(w.query("directory/bg:r", "SELECT owner FROM wasmkeep_directory_map")[0][0], "tenant/pusher/r");
    for (const e of ["edge/r.e1", "edge/r.e2"])
        assert.equal(JSON.parse(String(w.query(e, "SELECT config FROM wasmkeep_edge")[0][0])).room, "tenant/pusher/r");
    assert.equal(w.query("room/r", "SELECT name FROM sqlite_master WHERE name LIKE 'wasmkeep_room_s%' OR name LIKE 'wasmkeep_room_ops'").length, 0, "the room's legacy rows are gone");
    const state = w.as("pusher/r", () => new ScaleHost({ directory: "directory/bg:r", edgeClass: "edge", capacity: 5 }).load()!);
    assert.deepEqual(state.children.map(c => [c.name, c.hi]), [["edge/r.e1", MID], ["edge/r.e2", undefined]]);
    assert.equal(state.op, undefined, "the pending split finished in the pusher");
    assert.equal(state.next, 3, "the child counter moved with the state");
    // The edges now take the pusher's pushes: the backlog arrived.
    assert.equal(JSON.parse(String(w.query("edge/r.e1", "SELECT config FROM wasmkeep_edge")[0][0])).hi, MID.toString(16).padStart(16, "0"));
    // Every step repeats harmlessly: the room hands over again, the edge and map answer 200.
    w.as("pusher/r", () => {
        const again = new Writer().u64(1).bytes(toBytes("directory/bg:r")).u64(2).bytes(toBytes("edge/r.e1")).bytes(toBytes("edge/r.e2")).finish();
        assert.equal(w.call("room/r", { method: "POST", path: "/wasmkeep-room/v1/adopt", headers: [], body: again }).status, 200);
    });
    void pokes;
}
test("a 0.6.0 room's group moves to its pusher", () => migrate(false));
test("a 0.6.0 room's pending split resumes in the pusher after the move", () => migrate(true));
test("a new group: nothing to adopt, the pusher boots and owns the map", () => {
    db.install(":memory:");
    alarms.clearAlarm();
    const w = new World();
    w.add("directory/bg:r", defineDirectory());
    w.add("edge/r.e1", defineEdge());
    w.add("room/r", define());
    w.add("pusher/r", definePusher({ scaling: { directory: "directory/bg:r", edgeClass: "edge", capacity: 5 } }));
    w.alarm("pusher/r"); // As a join's poke would.
    for (let i = 0; i < 6 && alarms.pending !== undefined; i++)
        w.alarm("pusher/r");
    assert.equal(alarms.pending, undefined, "the booted group goes idle");
    assert.equal(w.query("directory/bg:r", "SELECT owner FROM wasmkeep_directory_map")[0][0], "tenant/pusher/r");
    // It said so to the room (not get-alarm's answer, the delivered alarm): a publish pokes it.
    w.add("probe/p", { fetch: () => ({ status: 200 }) });
    w.as("probe/p", () => new Client("room/r").publish([{ body: "m" }]));
    assert.equal(w.log.at(-1), "pusher/r/wasmkeep-pusher/v1/poke");
    assert.equal(JSON.parse(String(w.query("edge/r.e1", "SELECT config FROM wasmkeep_edge")[0][0])).room, "tenant/pusher/r");
});
test("edge handover and directory transfer: the controller only, idempotent", () => {
    db.install(":memory:");
    const w = new World();
    w.add("directory/bg:r", defineDirectory());
    w.add("edge/r.e1", defineEdge());
    w.as("room/r", () => {
        w.call("directory/bg:r", { method: "POST", path: "/wasmkeep-directory/v1/create", headers: [], body: new Writer().bytes(toBytes("edge/r.e1")).bytes(new Uint8Array()).finish() });
        w.call("edge/r.e1", { method: "POST", path: "/wasmkeep-edge/v1/init", headers: [], body: toBytes(JSON.stringify({ room: "tenant/room/r", lo: "0000000000000000", state: "active" })) });
    });
    const transfer = new Writer().bytes(toBytes("tenant/pusher/r")).finish(), handover = toBytes(JSON.stringify({ room: "tenant/pusher/r" }));
    const as = (who: string) => w.as(who, () => [
        w.call("directory/bg:r", { method: "POST", path: "/wasmkeep-directory/v1/transfer", headers: [], body: transfer }).status,
        w.call("edge/r.e1", { method: "POST", path: "/wasmkeep-edge/v1/handover", headers: [], body: handover }).status,
    ]);
    assert.deepEqual(as("thief/r"), [403, 403]);
    assert.deepEqual(as("room/r"), [200, 200]);
    assert.deepEqual(as("room/r"), [200, 200], "repeated: already done");
    assert.equal(w.query("directory/bg:r", "SELECT owner, version FROM wasmkeep_directory_map")[0].join(), "tenant/pusher/r,1");
    // The old controller lost the edge; the new one has it.
    const range = toBytes(JSON.stringify({ room: "tenant/room/r", lo: "0000000000000000", state: "active" }));
    assert.equal(w.as("room/r", () => w.call("edge/r.e1", { method: "POST", path: "/wasmkeep-edge/v1/range", headers: [], body: range }).status), 403);
    const mine = toBytes(JSON.stringify({ room: "tenant/pusher/r", lo: "0000000000000000", state: "active" }));
    assert.equal(w.as("pusher/r", () => w.call("edge/r.e1", { method: "POST", path: "/wasmkeep-edge/v1/range", headers: [], body: mine }).status), 200);
});
test("wake: the pusher clocks itself while behind, says idle at the head, and only then is poked", () => {
    db.install(":memory:");
    alarms.clearAlarm();
    const w = new World();
    let acked = 0, lagging = 0;
    w.add("edge/f", { fetch(req) {
            if (req.path.endsWith("/init"))
                return { body: "{}" };
            const p = JSON.parse(req.text());
            if (p.messages.length)
                acked = p.first + p.messages.length - 1;
            return { body: JSON.stringify({ e: p.e, n: acked, lagging, sockets: 0 }) };
        } });
    w.add("room/r", define());
    w.add("pusher/r", definePusher({ edges: ["edge/f"] }));
    w.add("probe/p", { fetch: () => ({ status: 200 }) });
    const pokes = () => w.log.filter(l => l === "pusher/r/wasmkeep-pusher/v1/poke").length;
    const publish = () => w.as("probe/p", () => new Client("room/r").publish([{ body: "m" }]));
    publish();
    assert.equal(pokes(), 1, "a cold room asks for a poke");
    let fired = 0;
    const fire = () => { fired++; assert(alarms.pending !== undefined, `an alarm stands before firing #${fired}: ${w.log.slice(-6).join(" ")}`); w.alarm("pusher/r"); };
    fire(); // init
    fire(); // header: the edge's position
    fire(); // the message; the edge is at the head: idle pull
    assert.equal(acked, 1);
    // Idle after something new: one safety round 5 s on, for a lost poke.
    assert(alarms.pending !== undefined && alarms.pending > BigInt(Date.now() + 4000), "one safety round");
    assert(w.log.at(-1)?.endsWith("/pull"), "the round ended with the idle pull");
    publish();
    assert.equal(pokes(), 2, "an idle pusher is poked");
    lagging = 1;
    fire(); // pushes 2; a socket lags: the pusher keeps its 20 ms clock
    assert(alarms.pending !== undefined);
    publish();
    assert.equal(pokes(), 2, "a busy pusher is not poked");
    lagging = 0;
    fire();
    assert.equal(acked, 3);
    fire(); // the safety round finds nothing new: no alarm is left
    assert.equal(alarms.pending, undefined, "a quiet room without sockets keeps no alarm");
    publish();
    assert.equal(pokes(), 3);
});
test("a failed scale step never stalls delivery: the pusher still says idle, a publish still pokes", () => {
    db.install(":memory:");
    alarms.clearAlarm();
    const w = new World();
    // The directory is down: the boot's second step (create) fails every time.
    w.add("directory/bg:r", { fetch: () => ({ status: 503, body: "down" }) });
    let acked = 0;
    w.add("edge/r.e1", { fetch(req) {
            if (!req.path.endsWith("/push"))
                return { body: '{"sockets":0}' };
            const p = JSON.parse(req.text());
            if (p.messages.length)
                acked = p.first + p.messages.length - 1;
            return { body: JSON.stringify({ e: p.e, n: acked, lagging: 0, sockets: 1 }) };
        } });
    w.add("room/r", define());
    w.add("pusher/r", definePusher({ scaling: { directory: "directory/bg:r", edgeClass: "edge", capacity: 5 } }));
    w.add("probe/p", { fetch: () => ({ status: 200 }) });
    w.alarm("pusher/r"); // boot step 0: the edge
    w.alarm("pusher/r"); // boot step 1 fails: retried 2 s later
    assert(alarms.pending !== undefined && alarms.pending > BigInt(Date.now() + 1000), "the step's retry stands");
    assert(w.log.at(-1)?.endsWith("/pull"), "the round ended with the idle pull");
    w.as("probe/p", () => new Client("room/r").publish([{ body: "m" }]));
    assert.equal(w.log.at(-1), "pusher/r/wasmkeep-pusher/v1/poke", "a publish pokes the pusher");
    assert(alarms.pending! <= BigInt(Date.now() + 20));
    w.alarm("pusher/r");
    assert.equal(acked, 1, "the message reached the edge while the step keeps failing");
});
for (const sockets of [1, 0])
test(`a quiet room ${sockets ? "with" : "without"} sockets: ${sockets ? "an idle alarm delivers a publish whose poke was lost" : "no alarm is left"}`, () => {
    db.install(":memory:");
    alarms.clearAlarm();
    const w = new World();
    let acked = 0;
    w.add("edge/f", { fetch(req) {
            if (req.path.endsWith("/init"))
                return { body: "{}" };
            const p = JSON.parse(req.text());
            if (p.messages.length)
                acked = p.first + p.messages.length - 1;
            return { body: JSON.stringify({ e: p.e, n: acked, lagging: 0, sockets }) };
        } });
    w.add("room/r", define());
    const pusher = definePusher({ edges: ["edge/f"] });
    w.add("pusher/r", pusher);
    w.add("probe/p", { fetch: () => ({ status: 200 }) });
    const publish = () => w.as("probe/p", () => new Client("room/r").publish([{ body: "m" }]));
    publish();
    for (let i = 0; i < 8 && alarms.pending !== undefined && alarms.pending <= BigInt(Date.now() + 5000); i++)
        w.alarm("pusher/r");
    assert.equal(acked, 1);
    if (!sockets) {
        assert.equal(alarms.pending, undefined, "an empty group keeps no alarm");
        return;
    }
    // Quiet: the safety round found nothing new; one idle alarm stays, at most 60 s out.
    assert(alarms.pending !== undefined && alarms.pending <= BigInt(Date.now() + 60000), "an idle alarm while sockets are held");
    // The pusher cannot be reached: the poke fails (three tries) and is lost.
    w.add("pusher/r", { fetch: () => { throw { payload: { tag: "failed", val: "node down" } }; } });
    publish();
    assert.equal(w.log.filter(l => l === "pusher/r/wasmkeep-pusher/v1/poke").length, 1 + 3, "a failed poke is tried three times");
    // Nothing else ever comes: the idle alarm, on the pusher's next node, delivers it.
    w.add("pusher/r", pusher);
    w.alarm("pusher/r");
    assert.equal(acked, 2, "the acknowledged publish reached the edge with no later publish");
});
