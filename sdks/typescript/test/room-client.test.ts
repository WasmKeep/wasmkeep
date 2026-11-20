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
    tick(ms: number) {
        const end = this.time + ms;
        for (;;) {
            const task = [...this.tasks].sort((a, b) => a[1].at - b[1].at)[0];
            if (!task || task[1].at > end)
                break;
            this.time = task[1].at;
            this.tasks.delete(task[0]);
            task[1].fn();
        }
        this.time = end;
    }
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
const settle = async () => {
    for (let i = 0; i < 20; i++)
        await Promise.resolve();
};
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
for (const jsonTurns of [0, 30])
    test(`HTTP Retry-After, pagination, held frames and cancellation (${jsonTurns} JSON turns)`, async () => {
        const clock = new FakeClock(), ws = new FakeSocket(), queries: string[] = [];
        const seen: number[] = [];
        let delivered!: () => void;
        const delivery = new Promise<void>(resolve => { delivered = resolve; });
        const c = new RoomClient({ join: "wss://x/join", history: "https://x/history", last: { e: 1, n: 0 }, clock, random: () => .5, socket: () => ws, onMessage: p => { seen.push(p.n); if (p.n === 3) delivered(); }, fetch: (async (url: unknown) => {
                queries.push(String(url));
                if (queries.length === 1)
                    return new Response(null, { status: 429, headers: { "Retry-After": "2" } });
                const response = new Response(JSON.stringify({ head: { e: 1, n: 2 }, messages: [{ position: { e: 1, n: queries.length - 1 }, at: 0, body: "x" }], more: queries.length === 2 }));
                const json = response.json.bind(response);
                response.json = async () => { for (let i = 0; i < jsonTurns; i++) await Promise.resolve(); return json(); };
                return response;
            }) as typeof fetch });
        c.connect();
        ws.frame({ t: "behind", e: 1, n: 2 });
        ws.frame({ t: "m", e: 1, b: 0, n: 3, m: [{ a: 0, d: "live" }] });
        await settle();
        clock.tick(2249);
        await settle();
        assert.equal(queries.length, 1);
        clock.tick(1);
        // JSON decoding and pagination can take any number of promise turns.
        // Completion is the held live frame reaching the application.
        let timeout: ReturnType<typeof setTimeout> | undefined;
        try {
            await Promise.race([delivery, new Promise<never>((_, reject) => {
                timeout = setTimeout(() => reject(Error("held frame was not delivered")), 5000);
            })]);
        } finally {
            clearTimeout(timeout);
            c.close();
        }
        assert.deepEqual(seen, [1, 2, 3]);
        assert(queries[2].endsWith("after=1.1"));
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
test('repeat counter includes live items superseded by HTTP catch-up', async () => {
    const repeats: number[] = [], seen: number[] = [];
    const r = new Receiver({
        onMessage: p => { seen.push(p.n); },
        onRepeat: p => { repeats.push(p.n); },
        history: async () => ({ head: { e: 1, n: 3 }, more: false, messages: [1, 2, 3].map(n => ({ position: { e: 1, n }, at: 0, body: String(n) })) }),
    }, { e: 1, n: 1 });
    await r.receive({ t: 'm', e: 1, b: 0, n: 3, m: [{ a: 0, d: '3' }] });
    assert.deepEqual(seen, [2, 3]);
    assert.deepEqual(repeats, [1, 3]);
});
for (const mode of ["stalled", "throttled"] as const)
    for (const bodySize of [1, 65536])
        test(`bounded live backlog during ${mode} history (${bodySize} bytes) recovers from applied position`, async () => {
            const clock = new FakeClock(), sockets: FakeSocket[] = [], queries: string[] = [], signals: AbortSignal[] = [], seen: number[] = [];
            const c = new RoomClient({ join: "wss://x/join", history: "https://x/history", last: { e: 1, n: 1 }, clock, random: () => .5,
                socket: () => { const s = new FakeSocket(); sockets.push(s); return s; }, onMessage: p => { seen.push(p.n); },
                fetch: (async (url, init) => {
                    queries.push(String(url));
                    signals.push(init!.signal!);
                    if (queries.length === 1) {
                        if (mode === "throttled")
                            return new Response(null, { status: 429, headers: { "Retry-After": "3600" } });
                        return new Promise<Response>((_, reject) => init!.signal!.addEventListener("abort", () => reject(Error("aborted")), { once: true }));
                    }
                    return new Response(JSON.stringify({ head: { e: 1, n: 3 }, more: false, messages: [2, 3].map(n => ({ position: { e: 1, n }, body: "recovered", at: 0 })) }));
                }) as typeof fetch });
            c.connect();
            sockets[0].frame({ t: "behind", e: 1, n: 3 });
            await settle();
            for (let i = 0; i < 100; i++)
                sockets[0].frame({ t: "m", e: 1, b: 0, n: 3, m: [{ a: 0, d: "x".repeat(bodySize) }] });
            await settle();
            assert(c.receiver.pending.frames <= 64);
            assert(c.receiver.pending.bytes <= 1024 * 1024);
            assert(sockets[0].closed, "overflow closes live transport");
            assert(signals[0].aborted, "obsolete history is cancelled");
            assert.deepEqual(c.last, { e: 1, n: 1 });
            clock.tick(250);
            await settle();
            sockets[1].frame({ t: "behind", e: 1, n: 3 });
            await settle();
            assert(queries[1].endsWith("after=1.1"));
            assert.deepEqual(seen, [2, 3]);
            c.close();
            assert.equal(clock.tasks.size, 0);
        });
test("history deadline cancels a stalled fetch despite healthy live keepalives", async () => {
    const clock = new FakeClock(), ws = new FakeSocket();
    let signal: AbortSignal | undefined;
    const c = new RoomClient({ join: "wss://x/join", history: "https://x/history", last: { e: 1, n: 1 }, clock, random: () => .5, socket: () => ws,
        fetch: (async (_url, init) => {
            signal = init!.signal!;
            return new Promise<Response>((_, reject) => signal!.addEventListener("abort", () => reject(Error("aborted")), { once: true }));
        }) as typeof fetch });
    c.connect();
    ws.frame({ t: "behind", e: 1, n: 2 });
    await settle();
    for (let i = 0; i < 3; i++) {
        clock.tick(9999);
        ws.frame({ t: "pong", e: 1, n: 2 });
        await settle();
    }
    assert(!ws.closed);
    clock.tick(3);
    await settle();
    assert(signal!.aborted);
    assert(ws.closed);
    assert.deepEqual(c.last, { e: 1, n: 1 });
    c.close();
});
test("cancelled history cannot apply a late response or queued frames", async () => {
    let release!: (h: any) => void;
    const seen: number[] = [];
    const r = new Receiver({ history: () => new Promise(resolve => { release = resolve; }), onMessage: p => { seen.push(p.n); } }, { e: 1, n: 1 });
    const old = r.receive({ t: "behind", e: 1, n: 2 });
    await settle();
    r.receive({ t: "m", e: 1, b: 0, n: 3, m: [{ a: 0, d: "stale" }] });
    r.cancel();
    release({ head: { e: 1, n: 2 }, more: false, messages: [{ position: { e: 1, n: 2 }, at: 0, body: "stale" }] });
    let rejected = false;
    try {
        await old;
    }
    catch {
        rejected = true;
    }
    assert(rejected);
    assert.deepEqual(seen, []);
    assert.deepEqual(r.last, { e: 1, n: 1 });
    await r.receive({ t: "m", e: 1, b: 0, n: 2, m: [{ a: 0, d: "fresh" }] });
    assert.deepEqual(seen, [2]);
});
/** A page of `n` messages from `from`, `more` while below `head`. */
const page = (from: number, n: number, head: number) => new Response(JSON.stringify({ head: { e: 1, n: head }, more: from + n - 1 < head,
    messages: Array.from({ length: n }, (_, i) => ({ position: { e: 1, n: from + i }, at: 0, body: String(from + i) })) }));
test("a 5xx history answer keeps the socket, halves the page and catches up", async () => {
    const clock = new FakeClock(), ws = new FakeSocket(), queries: URL[] = [], seen: number[] = [], errors: unknown[] = [];
    const c = new RoomClient({ join: "wss://x/join", history: "https://x/history?name=a", last: { e: 1, n: 0 }, clock, random: () => .5, socket: () => ws,
        onMessage: p => { seen.push(p.n); }, onError: e => { errors.push(e); },
        fetch: (async (url: unknown) => {
            const u = new URL(String(url));
            queries.push(u);
            // The front's CPU budget cuts a full page, a 502, a 503 (a takeover: same page), a 504.
            if (queries.length <= 4)
                return new Response("internal error\n", { status: [500, 502, 503, 504][queries.length - 1] });
            const after = Number(u.searchParams.get("after")!.split(".")[1]), limit = Number(u.searchParams.get("limit") ?? 256);
            return page(after + 1, Math.min(limit, 600 - after), 600);
        }) as typeof fetch });
    c.connect();
    ws.onopen?.();
    // 600 behind, and live messages keep coming while it backs off.
    ws.frame({ t: "behind", e: 1, n: 600 });
    // Response bodies are read on the event loop: let it turn between ticks.
    const turn = async () => { await settle(); await new Promise(resolve => setTimeout(resolve, 0)); await settle(); };
    for (let t = 0; t < 120 && seen.length < 604; t++) {
        await turn();
        if (t % 10 === 0 && t < 40)
            ws.frame({ t: "m", e: 1, b: 0, n: 601 + t / 10, m: [{ a: 0, d: "live" }] });
        clock.tick(500);
    }
    assert.deepEqual(errors.map(String), []);
    assert.equal(ws.closed, false, "a 5xx never closes the socket");
    assert.deepEqual(seen, Array.from({ length: 604 }, (_, i) => i + 1));
    assert.deepEqual(queries.slice(0, 5).map(u => u.searchParams.get("limit")), [null, "128", "64", "64", "32"]);
    assert(queries.every(u => u.searchParams.get("name") === "a"));
    c.close();
});
test("a 4xx history answer still closes and reconnects", async () => {
    const clock = new FakeClock(), sockets: FakeSocket[] = [], errors: unknown[] = [];
    const c = new RoomClient({ join: "wss://x/join", history: "https://x/history", last: { e: 1, n: 0 }, clock, random: () => .5,
        socket: () => { const s = new FakeSocket(); sockets.push(s); return s; }, onError: e => { errors.push(e); },
        fetch: (async () => new Response(null, { status: 400 })) as typeof fetch });
    c.connect();
    sockets[0].frame({ t: "behind", e: 1, n: 2 });
    await settle();
    assert(sockets[0].closed);
    assert(/history answered 400/.test(String(errors[0])));
    c.close();
});
test("the reconnect attempt counter returns to 0 only once history caught up", async () => {
    const clock = new FakeClock(), sockets: FakeSocket[] = [], urls: string[] = [];
    let failing = true;
    const c = new RoomClient({ join: "wss://x/join", history: "https://x/history", last: { e: 1, n: 0 }, clock, random: () => .5,
        socket: url => { urls.push(url); const s = new FakeSocket(); sockets.push(s); return s; },
        fetch: (async () => failing ? new Response(null, { status: 500 }) : page(1, 5, 5)) as typeof fetch });
    c.connect();
    // Two failed opens: attempt 2, the next delay 0.5 * 1000 ms.
    sockets[0].onerror?.();
    clock.tick(250);
    sockets[1].onerror?.();
    clock.tick(500);
    assert.equal(sockets.length, 3);
    sockets[2].onopen?.();
    sockets[2].frame({ t: "ready", e: 1, n: 5 });
    // Ready for 20 s, but history keeps failing: still attempt 2.
    for (let i = 0; i < 20; i++) {
        await settle();
        clock.tick(1000);
    }
    assert.equal(sockets[2].closed, false);
    sockets[2].onclose?.({ code: 1006 });
    clock.tick(999);
    assert.equal(sockets.length, 3, "attempt 2 waits 1 s");
    clock.tick(1);
    assert.equal(sockets.length, 4);
    // Caught up, then ready for 10 s: back to attempt 0.
    failing = false;
    sockets[3].onopen?.();
    sockets[3].frame({ t: "ready", e: 1, n: 5 });
    for (let i = 0; i < 11; i++) {
        await settle();
        clock.tick(1000);
    }
    sockets[3].onclose?.({ code: 1006 });
    clock.tick(250);
    assert.equal(sockets.length, 5, "attempt 0 waits 250 ms");
    c.close();
});
test("an edge's announced position fetches history only above the client's own", async () => {
    const queries: unknown[] = [], seen: number[] = [];
    const r = new Receiver({ onMessage: p => { seen.push(p.n); }, history: async (p) => {
            queries.push(p);
            return { head: { e: 1, n: 375 }, messages: [{ position: { e: 1, n: 374 }, at: 0, body: "374" }, { position: { e: 1, n: 375 }, at: 0, body: "375" }], more: false };
        } }, { e: 1, n: 373 });
    // Equal: the client missed nothing, no request.
    await r.receive({ t: "ready", e: 1, b: 0, n: 373 });
    assert.deepEqual(queries, []);
    // Above: one history fetch from the client's own position.
    await r.receive({ t: "ready", e: 1, b: 0, n: 375 });
    assert.deepEqual(queries, [{ e: 1, n: 373 }]);
    assert.deepEqual(seen, [374, 375]);
    // Below (the push repeats what the client has): no request.
    await r.receive({ t: "ready", e: 1, b: 0, n: 374 });
    assert.deepEqual(queries, [{ e: 1, n: 373 }]);
    assert.deepEqual(r.last, { e: 1, n: 375 });
});
test("an announced position in the next epoch applies its base without a fetch", async () => {
    const lost: number[][] = [];
    const r = new Receiver({ onLost: (a, b) => { lost.push([a, b]); }, history: async () => { throw Error("unexpected fetch"); } }, { e: 1, n: 373 });
    await r.receive({ t: "ready", e: 2, b: 373, n: 373 });
    assert.deepEqual(r.last, { e: 2, n: 373 });
    const s = new Receiver({ onLost: (a, b) => { lost.push([a, b]); }, history: async () => { throw Error("unexpected fetch"); } }, { e: 1, n: 375 });
    await s.receive({ t: "ready", e: 2, b: 373, n: 373 });
    assert.deepEqual(lost, [[374, 375]]);
    assert.deepEqual(s.last, { e: 2, n: 373 });
});
test("a pong with no head is a keepalive only", async () => {
    const r = new Receiver({ history: async () => { throw Error("unexpected fetch"); } }, { e: 1, n: 3 });
    await r.receive({ t: "pong", e: 0, n: null } as never);
    assert.deepEqual(r.last, { e: 1, n: 3 });
    for (const frame of [{ t: "behind", e: 1, n: null }, { t: "ready", e: 1, b: -1, n: 3 }]) {
        let error: unknown;
        try {
            await r.receive(frame as never);
        }
        catch (e) {
            error = e;
        }
        assert(error instanceof Error && /invalid/.test(error.message));
    }
});
