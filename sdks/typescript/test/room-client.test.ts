import { test } from "node:test";
import assert from "node:assert/strict";
import { Receiver, backoff, joinURL } from "../src/room-client";
test("client drops repeats, fetches gaps and preserves reconnect position", async () => {
    const delivered: number[] = [], queries: unknown[] = [];
    const r = new Receiver({ onMessage(p) { delivered.push(p.n); }, history: async (p) => {
            queries.push(p);
            return { head: { e: 1, n: 3 }, messages: [{ position: { e: 1, n: 2 }, at: 0, body: "2" }], more: false };
        } });
    await r.receive({ t: "ready", e: 1, n: 0 });
    await r.receive({ t: "m", e: 1, b: 0, n: 1, m: [{ a: 0, d: "1" }] });
    await r.receive({ t: "m", e: 1, b: 0, n: 1, m: [{ a: 0, d: "1" }] });
    await r.receive({ t: "m", e: 1, b: 0, n: 3, m: [{ a: 0, d: "3" }] });
    assert.deepEqual(delivered, [1, 2, 3]);
    assert.deepEqual(queries, [{ e: 1, n: 1 }]);
    assert.equal(joinURL("wss://example/join?c=a", r.last), "wss://example/join?c=a&e=1&n=3");
});
test("client announces lost and reset and bounds full jitter", async () => {
    const lost: number[][] = [], resets: unknown[] = [];
    const r = new Receiver({ onLost(a, b) { lost.push([a, b]); }, onReset(p) { resets.push(p); }, history: async () => ({ head: { e: 4, n: 20 }, reset: { e: 4, n: 20 }, messages: [{ position: { e: 4, n: 20 }, at: 0, body: "20" }], more: false }) });
    await r.receive({ t: "ready", e: 1, n: 5 });
    await r.receive({ t: "m", e: 2, b: 3, n: 4, m: [{ a: 0, d: "new" }] });
    assert.deepEqual(lost, [[4, 5]]);
    await r.receive({ t: "behind", e: 4, n: 20 });
    assert.deepEqual(resets, [{ e: 4, n: 20 }]);
    for (let k = 0; k < 100; k++) {
        assert.equal(backoff(k, () => 0), 0);
        assert(backoff(k, () => .999) < Math.min(30000, 500 * 2 ** k));
    }
    assert.equal(backoff(0, () => .5, 4801), 1000);
    assert.equal(backoff(0, () => .5, 1012), 1000);
});
test("invalid frames cannot change the last position", async () => {
    const r = new Receiver({ history: async () => { throw Error("unexpected fetch"); } }, { e: 1, n: 1 });
    for (const frame of [null, { t: "what" }, { t: "ready", e: 1, n: -1 }, { t: "m", e: 1, b: 0, n: 2, m: [{ a: 0, d: 123 }] }, { t: "ready", e: 1, n: Number.MAX_SAFE_INTEGER + 1 }]) {
        let error: unknown;
        try {
            await r.receive(frame as never);
        }
        catch (e) {
            error = e;
        }
        assert(error instanceof Error && /invalid/.test(error.message));
        assert.deepEqual(r.last, { e: 1, n: 1 });
    }
});
test("multi-page recovery holds live frames and applies lowest lost base", async () => {
    const seen: number[] = [], lost: number[][] = [], after: unknown[] = [];
    const r = new Receiver({ onMessage: p => { seen.push(p.n); }, onLost: (a, b) => { lost.push([a, b]); }, history: async (p) => {
            after.push(p);
            return after.length === 1 ? { head: { e: 4, n: 4 }, lost: { e: 3, base: 1 }, messages: [{ position: { e: 3, n: 2 }, at: 0, body: "2" }], more: true } : { head: { e: 4, n: 4 }, messages: [{ position: { e: 4, n: 3 }, at: 0, body: "3" }], more: false };
        } }, { e: 1, n: 5 });
    await Promise.all([r.receive({ t: "behind", e: 4, n: 4 }), r.receive({ t: "m", e: 4, b: 2, n: 4, m: [{ a: 0, d: "4" }] })]);
    assert.deepEqual(seen, [2, 3, 4]);
    assert.deepEqual(lost, [[2, 5]]);
    assert.deepEqual(after, [{ e: 1, n: 5 }, { e: 3, n: 2 }]);
});
import { RoomClient, type Clock, type Socket } from "../src/room-client";
class FakeClock implements Clock {
    time = 0;
    next = 0;
    tasks = new Map<number, {
        at: number;
        fn: () => void;
    }>();
    now() { return this.time; }
    set(fn: () => void, ms: number) { const id = ++this.next; this.tasks.set(id, { at: this.time + ms, fn }); return id; }
    clear(id: unknown) { this.tasks.delete(id as number); }
    tick(ms: number) { const end = this.time + ms; for (;;) {
        const task = [...this.tasks].sort((a, b) => a[1].at - b[1].at)[0];
        if (!task || task[1].at > end)
            break;
        this.time = task[1].at;
        this.tasks.delete(task[0]);
        task[1].fn();
    } this.time = end; }
}
class FakeSocket implements Socket {
    onopen: Socket["onopen"] = null;
    onmessage: Socket["onmessage"] = null;
    onclose: Socket["onclose"] = null;
    onerror: Socket["onerror"] = null;
    sent: string[] = [];
    closed = false;
    send(s: string) { this.sent.push(s); }
    close() { this.closed = true; }
    frame(f: unknown) { this.onmessage?.({ data: JSON.stringify(f) }); }
}
const settle = async () => { for (let i = 0; i < 20; i++)
    await Promise.resolve(); };
test("clock: keepalive, timeout, moved jitter, stable reset, cancellation", async () => {
    const clock = new FakeClock(), sockets: FakeSocket[] = [], urls: string[] = [];
    const c = new RoomClient({ join: "wss://x/join", history: "https://x/history", clock, random: () => .5, socket: url => { urls.push(url); const s = new FakeSocket(); sockets.push(s); return s; } });
    c.connect();
    sockets[0].onopen?.();
    sockets[0].frame({ t: "ready", e: 1, n: 2 });
    await settle();
    clock.tick(45000);
    assert.deepEqual(sockets[0].sent, ['{"t":"ping"}']);
    clock.tick(10000);
    assert(sockets[0].closed);
    clock.tick(249);
    assert.equal(sockets.length, 1);
    clock.tick(1);
    assert.equal(urls[1], "wss://x/join?e=1&n=2");
    sockets[1].onopen?.();
    sockets[1].frame({ t: "ready", e: 1, n: 2 });
    await settle();
    clock.tick(10000);
    sockets[1].onclose?.({ code: 4801 });
    clock.tick(999);
    assert.equal(sockets.length, 2);
    clock.tick(1);
    assert.equal(sockets.length, 3);
    c.close();
    sockets[2].onopen?.();
    clock.tick(100000);
    assert.equal(clock.tasks.size, 0);
    assert.equal(sockets.length, 3);
});
test("HTTP Retry-After, pagination, held frames and cancellation", async () => {
    const clock = new FakeClock(), ws = new FakeSocket(), queries: string[] = [];
    const seen: number[] = [];
    const c = new RoomClient({ join: "wss://x/join", history: "https://x/history", last: { e: 1, n: 0 }, clock, random: () => .5, socket: () => ws, onMessage: p => { seen.push(p.n); }, fetch: (async (url: unknown) => {
            queries.push(String(url));
            if (queries.length === 1)
                return new Response(null, { status: 429, headers: { "Retry-After": "2" } });
            return new Response(JSON.stringify({ head: { e: 1, n: 2 }, messages: [{ position: { e: 1, n: queries.length - 1 }, at: 0, body: "x" }], more: queries.length === 2 }));
        }) as typeof fetch });
    c.connect();
    ws.frame({ t: "behind", e: 1, n: 2 });
    ws.frame({ t: "m", e: 1, b: 0, n: 3, m: [{ a: 0, d: "live" }] });
    await settle();
    clock.tick(2249);
    await settle();
    assert.equal(queries.length, 1);
    clock.tick(1);
    await settle();
    assert.deepEqual(seen, [1, 2, 3]);
    assert(queries[2].endsWith("after=1.1"));
    c.close();
    assert.equal(clock.tasks.size, 0);
});
test("history is authoritative when a current-epoch ready precedes retained rows", async () => {
    const seen: number[] = [];
    const r = new Receiver({ onMessage: p => { seen.push(p.n); }, history: async () => ({ head: { e: 2, n: 2 }, messages: [{ position: { e: 1, n: 1 }, body: "old", at: 0 }, { position: { e: 2, n: 2 }, body: "new", at: 0 }], more: false }) }, { e: 2, n: 0 });
    await r.receive({ t: "m", e: 1, b: 0, n: 1, m: [{ a: 0, d: "old" }] });
    assert.deepEqual(seen, [1, 2]);
});
import * as browser from "../src/room-client";
test("front history adapter preserves R2 identities and refuses unsafe integers", () => {
    const convert = (browser as any).historyJSON;
    assert.equal(typeof convert, "function");
    assert.deepEqual(convert({ head: { e: 2n, n: 1n }, messages: [{ position: { e: 1n, n: 1n }, at: 42n, id: "a", body: "old" }], more: false }), { head: { e: 2, n: 1 }, messages: [{ position: { e: 1, n: 1 }, at: 42, id: "a", body: "old" }], more: false });
    assert.throws(() => convert({ head: { e: 1n, n: 9007199254740993n }, messages: [], more: false }));
});
test("failed opens back off; stale callbacks cannot stop a replacement socket", async () => {
    const clock = new FakeClock(), sockets: FakeSocket[] = [];
    const c = new RoomClient({ join: "wss://x/join", history: "https://x/history", clock, random: () => .5, socket: () => { const s = new FakeSocket(); sockets.push(s); return s; } });
    c.connect();
    sockets[0].onerror?.();
    clock.tick(250);
    assert.equal(sockets.length, 2);
    sockets[1].onerror?.();
    clock.tick(499);
    assert.equal(sockets.length, 2);
    clock.tick(1);
    assert.equal(sockets.length, 3);
    sockets[0].onclose?.({ code: 1006 });
    sockets[0].onopen?.();
    clock.tick(500);
    assert.equal(sockets.length, 3);
    c.close();
});
test("503 HTTP-date retry is abortable and closes without another fetch", async () => {
    const clock = new FakeClock(), ws = new FakeSocket();
    let calls = 0;
    const c = new RoomClient({ join: "wss://x/join", history: "https://x/history", last: { e: 1, n: 0 }, clock, random: () => 0, socket: () => ws, fetch: (async () => { calls++; return new Response(null, { status: 503, headers: { "Retry-After": new Date(3000).toUTCString() } }); }) as typeof fetch });
    c.connect();
    ws.frame({ t: "behind", e: 1, n: 1 });
    await settle();
    clock.tick(2999);
    await settle();
    assert.equal(calls, 1);
    c.close();
    await settle();
    clock.tick(100000);
    assert.equal(calls, 1);
    assert.equal(clock.tasks.size, 0);
});
import * as frontBroadcast from "../src/broadcast-client";
test("the history adapter is available to a front without browser globals", () => {
    assert.equal(typeof (frontBroadcast as any).historyJSON, "function");
});
