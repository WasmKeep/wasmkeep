#!/usr/bin/env node
import { publishWithRetry } from './room-publish.mjs';
import { deliveryOrder, historyCoverage } from './room-order.mjs';
// Remote ADR 0040 driver. No server lifecycle or benchmark result publication.
import { parseArgs } from 'node:util';
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';
import { writeFile, rename, readFile } from 'node:fs/promises';
import { readdirSync } from 'node:fs';
import { setTimeout as sleep, setImmediate as yieldLoop } from 'node:timers/promises';
import { randomUUID, randomBytes, createHash } from 'node:crypto';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import http from 'node:http';
import https from 'node:https';

const { values: v } = parseArgs({ options: {
  url: { type: 'string', multiple: true, default: [] }, clients: { type: 'string', default: '24' },
  duration: { type: 'string' }, rate: { type: 'string', default: '2' },
  'publish-rate': { type: 'string' }, 'payload-bytes': { type: 'string' },
  publishers: { type: 'string', default: '1' }, out: { type: 'string' },
  ramp: { type: 'string' }, 'stalled-readers': { type: 'string', default: '0' },
  'stall-after': { type: 'string', default: '2' }, 'stall-for': { type: 'string', default: '10' },
  'history-concurrency': { type: 'string', default: '256' },
  'metrics-url': { type: 'string', multiple: true, default: [] },
  'reconnect-every': { type: 'string', default: '0' }, smoke: { type: 'boolean' },
  help: { type: 'boolean' },
} });
if (v.help) {
  console.log(`node bench/room-group.mjs --url https://front.example [--url ...] --out result.json
  --clients N              clients admitted before publishing starts (default 24)
  --duration S             publishing seconds (default 10; at least the ramp length)
  --rate R                 aggregate publishes/s, shared by the publishers (default 2)
  --publish-rate R         publishes/s per publisher (replaces --rate); --publishers 0 makes a
                           clients-only generator that checks completeness against the room head
  --publishers P           paced HTTP publishers (default 1)
  --payload-bytes N        message text size in bytes (default: a few; the example front accepts at most 2000)
  --ramp STEPS             comma list from join:N:SECONDS, hold:SECONDS, leave:M:SECONDS, e.g.
                           join:8000:60,hold:30,leave:8000:60; steps run from the publishing start
  --stalled-readers K      the last K initial clients stop reading, then resume
  --stall-after S          seconds after publishing starts (default 2); --stall-for S (default 10)
  --history-concurrency N  history requests in flight per process (default 256), queue wait reported
  --url URL (repeated)     clients and publishers round robin over the targets
  --metrics-url URL ...    optional Prometheus pages; --reconnect-every S drops a socket every S seconds
  --smoke                  loopback-only bounded correctness run`);
  process.exit(0);
}
function number(key, min, max, integer = false) {
  const n = Number(v[key]);
  if (!Number.isFinite(n) || n < min || n > max || (integer && !Number.isInteger(n))) throw Error(`invalid --${key}`);
  return n;
}
function parseRamp(text) {
  if (!text) return [];
  return text.split(',').map(part => {
    const f = part.split(':'), kind = f[0], num = x => { const n = Number(x); if (x === undefined || x === '' || !Number.isFinite(n) || n <= 0) throw Error(`invalid --ramp step ${part}`); return n; };
    if (kind === 'hold' && f.length === 2) return { kind, n: 0, t: num(f[1]) };
    if ((kind === 'join' || kind === 'leave') && f.length === 3) { const n = num(f[1]); if (!Number.isInteger(n)) throw Error(`invalid --ramp step ${part}`); return { kind, n, t: num(f[2]) }; }
    throw Error(`invalid --ramp step ${part}`);
  });
}
const initial = number('clients', 1, 128000, true), rate = number('rate', 0.01, 100000), publishers = number('publishers', 0, 1000, true);
const perPublisherRate = v['publish-rate'] === undefined ? rate / Math.max(1, publishers) : number('publish-rate', 0.01, 100000);
const aggregateRate = perPublisherRate * publishers;
const payloadBytes = v['payload-bytes'] === undefined ? 0 : number('payload-bytes', 1, 1048576, true);
const stalledCount = number('stalled-readers', 0, 128000, true), stallAfter = number('stall-after', 0, 86400), stallFor = number('stall-for', 0.1, 86400);
const historyConcurrency = number('history-concurrency', 1, 100000, true);
const churn = number('reconnect-every', 0, 86400);
const ramp = parseRamp(v.ramp);
const rampSeconds = ramp.reduce((a, s) => a + s.t, 0);
const duration = v.duration === undefined ? (rampSeconds || 10) : number('duration', 1, 86400);
if (duration < rampSeconds) throw Error('--duration must cover the --ramp steps');
if (stalledCount > initial) throw Error('--stalled-readers cannot exceed --clients');
let level = initial, peak = initial, joinedTotal = initial;
for (const s of ramp) { if (s.kind === 'join') { level += s.n; joinedTotal += s.n; } else if (s.kind === 'leave') level -= s.n; peak = Math.max(peak, level); if (level < 0) throw Error('--ramp leaves more clients than joined'); }
if (joinedTotal > 128000) throw Error('too many clients for one process');
const bases = v.url.map(u => new URL(u));
if (!bases.length) throw Error('--url is required');
for (const base of bases) if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash) throw Error('URL must be an HTTP(S) front without credentials/query/fragment');
if (!v.out) throw Error('--out is required (use scratch storage; review before publication)');
if (v.smoke && (!bases.every(b => ['localhost', '127.0.0.1', '[::1]'].includes(b.hostname)) || joinedTotal > 32 || duration > 15 || aggregateRate > 5 || publishers > 2 || stalledCount > 2 || payloadBytes > 65536)) throw Error('smoke requires loopback, <=32 clients in total, <=15 seconds, <=5 messages/s, <=2 publishers, <=2 stalled readers, <=64 KiB messages');
// Each client holds one socket; history requests, publishers and the runtime need more.
const procSelf = name => ['', 'proc', 'self', name].join('/');
async function softFdLimit() { try { const m = (await readFile(procSelf('limits'), 'utf8')).match(new RegExp('^Max open files\\s+(\\S+)', 'm')); return m ? (m[1] === 'unlimited' ? Infinity : Number(m[1])) : null; } catch { return null; } }
const fdLimit = await softFdLimit(), fdNeeded = peak + historyConcurrency + publishers + 64;
if (fdLimit !== null && fdLimit < fdNeeded) throw Error(`open-file soft limit ${fdLimit} is below the ${fdNeeded} this run needs (peak clients + history concurrency + publishers + 64); raise it for this shell (ulimit -n) or split the run over more processes`);
const require = createRequire(new URL('../sdks/typescript/package.json', import.meta.url));
const { build } = require('esbuild');
const bundle = await build({ entryPoints: [new URL('../sdks/typescript/src/room-client.ts', import.meta.url).pathname], bundle: true, write: false, format: 'esm', platform: 'node' });
const { RoomClient } = await import(`data:text/javascript;base64,${Buffer.from(bundle.outputFiles[0].contents).toString('base64')}`);
// Logarithmic buckets: bounded memory; percentile upper bounds within 1% or 1ms.
class Histogram {
  bins = new Map(); count = 0; max = 0;
  add(ms) { const b = Math.ceil(Math.log(Math.max(1, ms)) / Math.log(1.01)); this.bins.set(b, (this.bins.get(b) ?? 0) + 1); this.count++; if (ms > this.max) this.max = ms; }
  summary() { const entries = [...this.bins].sort((a, b) => a[0] - b[0]); return Object.fromEntries([['count', this.count], ...[50, 95, 99].map(p => { let n = 0; const found = entries.find(([, k]) => (n += k) >= Math.ceil(this.count * p / 100)); return [`p${p}_ms`, found ? 1.01 ** found[0] : null]; }), ['max_ms', this.count ? this.max : null]]); }
}
const COUNTERS = ['deliveries', 'repeats', 'discarded_catchup', 'discarded_live', 'history_requests', 'gaps_recovered', 'recovered_messages', 'void_numbers', 'resets', 'reconnect_attempts', 'close_4801', 'close_4802', 'errors', 'publish_ok', 'publish_failed', 'publish_unacknowledged', 'publish_retries', 'forced_disconnects', 'smoke_dropped_frames', 'clients_joined', 'clients_left', 'stall_pauses', 'stall_resumes'];
const zero = () => Object.fromEntries(COUNTERS.map(k => [k, 0]));
const counters = zero();
const delivery = new Histogram(), reconnect = new Histogram(), historyWait = new Histogram();
const stalledDelivery = new Histogram(), stalledReconnect = new Histogram(), stalledCatchup = new Histogram();
// Phases attribute counters and latencies to the part of the schedule they happened in.
const phases = [];
let phase;
const newPhase = name => ({ name, start_s: (performance.now() - start) / 1000, end_s: null, ready_at_end: null, active_at_end: null, counters: zero(), delivery: new Histogram(), reconnect: new Histogram() });
function setPhase(name) {
  if (phase) { phase.end_s = (performance.now() - start) / 1000; phase.ready_at_end = readyCount; phase.active_at_end = activeCount; }
  phases.push(phase = newPhase(name));
}
const bump = (key, n = 1) => { counters[key] += n; phase.counters[key] += n; };
const acknowledged = [], unacknowledged = [], publishFailuresByStatus = {};
const run = randomUUID().slice(0, 8), clients = [], sockets = new Set(), samples = [];
const prefix = `wkbench:${run}:`;
let stopped = false, readyCount = 0, activeCount = 0, openRaw = 0;
const stop = new AbortController();
const start = performance.now();
setPhase('0-admit');
const endpoint = (base, path, name) => {
  const u = new URL(path, base.href.endsWith('/') ? base.href : base.href + '/');
  u.searchParams.set('name', name);
  return u;
};
const boundedFetch = (url, init = {}) => fetch(url, { ...init, signal: AbortSignal.any([stop.signal, AbortSignal.timeout(2000), ...(init.signal ? [init.signal] : [])]) });
// Bounds this process's own history fan-in; the wait is reported, and a long one invalidates the run.
let historyInflight = 0; const historyWaiters = [];
async function historySlot(fn, signal) {
  const queued = performance.now();
  while (historyInflight >= historyConcurrency) await new Promise(r => historyWaiters.push(r));
  historyInflight++;
  try { signal?.throwIfAborted(); const waited = performance.now() - queued; if (waited >= 1) historyWait.add(waited); return await fn(); }
  finally { historyInflight--; historyWaiters.shift()?.(); }
}
// A WebSocket whose reads can stop, so the kernel buffers fill and the server sees a slow client.
class PausableSocket {
  onopen = null; onmessage = null; onclose = null; onerror = null;
  #socket = null; #buf = Buffer.alloc(0); #text = []; #ended = false; #owner; #closeCode;
  constructor(url, owner, closeCode) {
    this.#owner = owner; this.#closeCode = closeCode;
    const u = new URL(url); u.protocol = u.protocol === 'wss:' ? 'https:' : 'http:';
    const key = randomBytes(16).toString('base64');
    const req = (u.protocol === 'https:' ? https : http).request(u, { agent: false, headers: { Connection: 'Upgrade', Upgrade: 'websocket', 'Sec-WebSocket-Key': key, 'Sec-WebSocket-Version': '13' } });
    req.on('upgrade', (res, socket, head) => {
      if (res.headers['sec-websocket-accept'] !== createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64')) { socket.destroy(); return this.#fail(); }
      this.#socket = socket; openRaw++; socket.setNoDelay(true);
      socket.on('data', d => this.#data(d)); socket.on('error', () => {}); socket.on('close', () => { openRaw--; this.#end(1006); });
      if (head.length) this.#data(head);
      if (owner.stall) socket.pause();
      this.onopen?.();
    });
    req.on('response', res => { res.resume(); this.#fail(); });
    req.on('error', () => this.#fail());
    req.end();
  }
  pause() { this.#socket?.pause(); }
  resume() { this.#socket?.resume(); }
  #fail() { if (!this.#ended) this.onerror?.(); this.#end(1006); }
  #end(code) { if (this.#ended) return; this.#ended = true; this.#closeCode(code); this.onclose?.({ code }); }
  #frame(op, payload) {
    const n = payload.length, h = n < 126 ? Buffer.alloc(2) : Buffer.alloc(4), mask = randomBytes(4);
    h[0] = 0x80 | op; if (n < 126) h[1] = 0x80 | n; else { h[1] = 0x80 | 126; h.writeUInt16BE(n, 2); }
    const body = Buffer.from(payload); for (let i = 0; i < n; i++) body[i] ^= mask[i & 3];
    return Buffer.concat([h, mask, body]);
  }
  send(text) { if (this.#socket?.writable) this.#socket.write(this.#frame(1, Buffer.from(text))); }
  close() {
    if (this.#socket && !this.#ended) try { this.#socket.end(this.#frame(8, Buffer.alloc(0))); } catch {}
    this.#socket?.destroy(); this.#end(1005);
  }
  #data(chunk) {
    this.#buf = this.#buf.length ? Buffer.concat([this.#buf, chunk]) : chunk;
    for (;;) {
      const b = this.#buf; if (b.length < 2) return;
      let len = b[1] & 0x7f, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (b.length < off + len) return;
      const op = b[0] & 15, fin = b[0] & 0x80, payload = b.subarray(off, off + len);
      this.#buf = b.subarray(off + len);
      if (op === 1 || op === 0) { this.#text.push(payload); if (fin) { const data = Buffer.concat(this.#text).toString('utf8'); this.#text = []; this.onmessage?.({ data }); } }
      else if (op === 8) { this.#end(len >= 2 ? payload.readUInt16BE(0) : 1005); this.#socket.destroy(); return; }
      else if (op === 9 && this.#socket.writable) this.#socket.write(this.#frame(10, payload));
    }
  }
}
function addClient(stalledReader = false) {
  const i = clients.length, base = bases[i % bases.length], name = `b${run}-${i}`, recovered = new Set();
  // `repeats` counts what the application was handed twice; the SDK's own
  // discards (onRepeat) are split into catch-up overlaps and live re-sends.
  const order = deliveryOrder(), coverage = historyCoverage();
  const c = { client: null, ready: false, active: true, stalledReader, stall: false, raw: null };
  let attempts = 0, down, dropped = false;
  const count = code => { if (!stopped) { if (code === 4801) bump('close_4801'); if (code === 4802) bump('close_4802'); } };
  const join = endpoint(base, 'join', name); join.protocol = base.protocol === 'https:' ? 'wss:' : 'ws:';
  c.client = new RoomClient({ join: join.href, history: endpoint(base, 'history', name).href,
    socket(url) {
      if (stalledReader) return c.raw = new PausableSocket(url, c, count);
      const ws = new WebSocket(url); sockets.add(ws);
      // One synthetic lost frame proves the HTTP recovery instrumentation.
      if (v.smoke && i === 0) ws.addEventListener('message', event => {
        if (!dropped && JSON.parse(event.data).t === 'm') {
          dropped = true; bump('smoke_dropped_frames'); event.stopImmediatePropagation();
        }
      });
      ws.addEventListener('close', e => { sockets.delete(ws); count(e.code); });
      return ws;
    },
    async fetch(url, init) {
      bump('history_requests');
      return historySlot(async () => {
        const response = await boundedFetch(url, init);
        if (!response.ok) return response;
        const h = await response.json();
        recovered.clear(); coverage.page(h.messages);
        for (const m of h.messages) recovered.add(`${m.position.e}.${m.position.n}`);
        if (h.messages.length && new URL(url).searchParams.has('after')) bump('gaps_recovered');
        return { ok: true, status: response.status, json: async () => h };
      }, init?.signal);
    },
    onMessage(p, body) {
      bump('deliveries');
      if (order.message(p)) bump('repeats');
      if (recovered.delete(`${p.e}.${p.n}`)) bump('recovered_messages');
      // Timestamp originates on this load process, not a remote node's clock.
      // The stored body is the publisher's JSON, so the tag sits inside it.
      const at = body.indexOf(prefix);
      if (at >= 0) {
        const from = at + prefix.length, ms = performance.now() - Number(body.slice(from, body.indexOf(':', from)));
        (stalledReader ? stalledDelivery : phase.delivery).add(ms);
        if (!stalledReader) delivery.add(ms);
      }
    },
    onRepeat(p) { bump(coverage.covers(p) ? 'discarded_catchup' : 'discarded_live'); },
    onLost(a, b) { order.lost(a); bump('void_numbers', b - a + 1); },
    onReset(p) { order.reset(p); bump('resets'); }, onError() { if (!stopped) bump('errors'); },
    onState(state) {
      const was = c.ready; c.ready = state === 'ready'; readyCount += Number(c.ready) - Number(was);
      if (state === 'connecting' && attempts++ > 0) bump('reconnect_attempts');
      if (state === 'disconnected' && down === undefined) down = performance.now();
      if (state === 'ready' && down !== undefined) {
        const ms = performance.now() - down; down = undefined;
        if (stalledReader) stalledReconnect.add(ms); else { reconnect.add(ms); phase.reconnect.add(ms); }
      }
    },
  });
  clients.push(c); activeCount++; bump('clients_joined'); c.client.connect();
  return c;
}
function removeClient() {
  for (let k = clients.length - 1; k >= 0; k--) {
    const c = clients[k];
    if (c.active && !c.stalledReader) { c.active = false; activeCount--; bump('clients_left'); c.client.close(); return true; }
  }
  return false;
}
const sleepUntil = async t => { const d = t - performance.now(); if (d > 1) await sleep(d); else await yieldLoop(); };
const stalledClients = [];
for (let i = 0; i < initial; i++) {
  const c = addClient(i >= initial - stalledCount);
  if (c.stalledReader) stalledClients.push(c);
  // Bounded admission ramp, without blocking readers or catch-up bursts.
  await sleep(2);
}
const maxAck = () => acknowledged.reduce((m, a) => ({ e: Math.max(m.e, a.e), n: Math.max(m.n, a.n) }), { e: 0, n: 0 });
const reachedBy = target => p => !!p && p.e >= target.e && p.n >= target.n;
// The generator is invalid as a measurement when it, not the room, is the limit.
const loopDelay = monitorEventLoopDelay({ resolution: 10 }); loopDelay.enable();
let lastCpu = process.cpuUsage(), lastWall = performance.now(), peakRss = 0, peakFds = null, fdSamples = 0;
const generatorSample = () => {
  const cpu = process.cpuUsage(), wall = performance.now();
  const util = ((cpu.user - lastCpu.user) + (cpu.system - lastCpu.system)) / 1000 / Math.max(1, wall - lastWall);
  lastCpu = cpu; lastWall = wall;
  const mem = process.memoryUsage(); peakRss = Math.max(peakRss, mem.rss);
  const g = { cpu_cores: util, rss_bytes: mem.rss, heap_used_bytes: mem.heapUsed, loop_delay_p99_ms: loopDelay.percentile(99) / 1e6, loop_delay_max_ms: loopDelay.max / 1e6, sockets_open: sockets.size + openRaw, history_inflight: historyInflight };
  loopDelay.reset();
  if (fdSamples++ % 5 === 0) { try { g.open_fds = readdirSync(procSelf('fd')).length; peakFds = Math.max(peakFds ?? 0, g.open_fds); } catch {} }
  g.saturated = g.loop_delay_p99_ms >= 100 || g.cpu_cores >= 0.9;
  return g;
};
let tasks = [], readyAtStart = 0, runTasks = [];
try {
  const admissionDeadline = performance.now() + (v.smoke ? 3000 : 30000);
  while (readyCount < activeCount && performance.now() < admissionDeadline) await sleep(50);
  readyAtStart = readyCount;
  generatorSample();
  const publishingStart = performance.now(), end = publishingStart + duration * 1000;
  setPhase(ramp.length ? '1-' + ramp[0].kind : '1-steady');
  const period = 1000 / perPublisherRate;
  tasks = Array.from({ length: publishers }, (_, publisher) => (async () => {
    let seq = 0;
    const target = bases[publisher % bases.length];
    // Fixed schedule; a slow request delays later publishes but never causes a catch-up burst.
    const offset = publisher * period / publishers;
    const publishLimit = Math.ceil((duration * 1000 - offset) / period);
    let next = publishingStart + offset;
    // An integer limit avoids a floating-point extra slot at the end boundary.
    while (!stopped && next < end && seq < publishLimit) {
      await sleepUntil(next);
      const sent = performance.now(), id = `${run}-${publisher}-${seq++}`;
      const head = `${prefix}${sent}:`;
      const text = payloadBytes ? head + 'x'.repeat(Math.max(0, payloadBytes - head.length)) : head + 'payload';
      const body = JSON.stringify({ id, text });
      const retryBudget = v.smoke ? Math.max(1, Math.min(2000, end - performance.now())) : 30000;
      const positions = await publishWithRetry({
        body,
        // Smoke keeps its entire run below 30 seconds, including drain/cleanup.
        budgetMs: retryBudget,
        send: (body, remaining) => boundedFetch(endpoint(target, 'publish', `p${run}-${publisher}`), { method: 'POST', headers: { 'content-type': 'application/json' }, body, signal: AbortSignal.timeout(remaining) }),
        failure: status => { publishFailuresByStatus[status] = (publishFailuresByStatus[status] ?? 0) + 1; bump('publish_failed'); },
        retry: () => bump('publish_retries'),
      });
      if (positions) { acknowledged.push(...positions); bump('publish_ok'); }
      else { unacknowledged.push({ id, sent_ms: sent, retry_budget_ms: retryBudget }); bump('publish_unacknowledged'); }
      next = Math.max(next + period, performance.now());
    }
  })());
  // The client schedule grows and shrinks so edges split and drain during the run.
  runTasks.push((async () => {
    for (const [idx, s] of ramp.entries()) {
      if (idx) setPhase(`${idx + 1}-${s.kind}`);
      const t0 = performance.now(), t1 = t0 + s.t * 1000;
      for (let k = 0; k < s.n && !stopped && performance.now() < end; k++) {
        await sleepUntil(t0 + k * s.t * 1000 / s.n);
        if (s.kind === 'join') addClient(); else removeClient();
      }
      await sleepUntil(Math.min(t1, end));
    }
    if (ramp.length && performance.now() < end) setPhase('after-ramp');
  })());
  // Stalled readers stop reading, then resume and must catch up by number.
  for (const c of stalledClients) runTasks.push((async () => {
    await sleepUntil(publishingStart + stallAfter * 1000);
    if (stopped) return;
    c.stall = true; c.raw?.pause(); bump('stall_pauses');
    await sleepUntil(publishingStart + (stallAfter + stallFor) * 1000);
    c.stall = false; c.raw?.resume(); bump('stall_resumes');
    const resumed = performance.now(), target = maxAck(), reached = reachedBy(target);
    while (!stopped && !reached(c.client.last) && performance.now() < resumed + 30000) await sleep(50);
    if (reached(c.client.last)) stalledCatchup.add(performance.now() - resumed);
  })());
  let nextChurn = publishingStart + churn * 1000;
  while (performance.now() < end) {
    const sample = { elapsed_s: (performance.now() - start) / 1000, phase: phase.name, ready: readyCount, active: activeCount, edges: null, relays: null, fronts: [], nodes: [] };
    await Promise.all(bases.map(async (base, idx) => {
      try { const r = await boundedFetch(endpoint(base, 'stats', `b${run}`)); if (!r.ok) throw Error(); const s = await r.json(); sample.fronts[idx] = { edges: s.edges ?? null, relays: s.relays ?? null }; }
      catch { sample.fronts[idx] = { unavailable: true }; }
    }));
    const first = sample.fronts.find(f => !f.unavailable);
    if (first) { sample.edges = first.edges; sample.relays = first.relays; } else sample.stats_unavailable = true;
    for (const [node, url] of v['metrics-url'].entries()) {
      const values = { node, cpu_seconds: null, rss_bytes: null };
      try {
        const r = await boundedFetch(url); if (!r.ok) throw Error();
        const text = await r.text();
        for (const [metric, key] of [['process_cpu_seconds_total', 'cpu_seconds'], ['process_resident_memory_bytes', 'rss_bytes']]) {
          const match = text.match(new RegExp(`^${metric}(?:\\{[^}]*\\})?\\s+([0-9.eE+-]+)`, 'm'));
          if (match && Number.isFinite(Number(match[1]))) values[key] = Number(match[1]);
        }
      } catch { values.unavailable = true; }
      sample.nodes.push(values);
    }
    sample.generator = generatorSample();
    samples.push(sample);
    if (churn && performance.now() >= nextChurn) { const ws = [...sockets].find(s => s.readyState === WebSocket.OPEN); if (ws) { bump('forced_disconnects'); ws.close(); } nextChurn = performance.now() + churn * 1000; }
    await sleep(Math.min(1000, Math.max(0, end - performance.now())));
  }
  await Promise.all([...tasks, ...runTasks]);
  setPhase('drain');
  let target = maxAck(), headUnknown = false;
  if (!publishers) {
    // A clients-only process cannot know the other processes' acknowledgements: use the room head.
    try { const r = await boundedFetch(endpoint(bases[0], 'stats', `b${run}`)); const s = await r.json(); if (!Number.isSafeInteger(s.head?.e) || !Number.isSafeInteger(s.head?.n)) throw Error(); target = s.head; } catch { headUnknown = true; }
  }
  const reached = reachedBy(target);
  const drainDeadline = performance.now() + (v.smoke ? 3000 : 10000);
  while (clients.some(c => c.active && !reached(c.client.last)) && performance.now() < drainDeadline) await sleep(50);
  const finalCursors = clients.map(c => c.client.last ?? null);
  const incomplete = clients.flatMap((c, i) => c.active && !reached(finalCursors[i]) ? [i] : []);
  phase.end_s = (performance.now() - start) / 1000; phase.ready_at_end = readyCount; phase.active_at_end = activeCount;
  let commit = 'unknown';
  try { commit = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(); if (execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }).trim()) commit += '-dirty'; } catch {}
  const topology = samples.map(s => ({ elapsed_s: s.elapsed_s, phase: s.phase, active: s.active, edges: s.edges, relays: s.relays }));
  const judged = samples.length >= 5;
  const saturatedShare = judged ? samples.filter(s => s.generator.saturated).length / samples.length : 0;
  const reasons = [];
  if (judged && saturatedShare >= 0.1) reasons.push('load generator saturated (event-loop delay p99 >= 100 ms or CPU >= 0.9 core) in at least 10% of samples');
  if (historyWait.count && historyWait.summary().p99_ms >= 1000) reasons.push('history requests queued in this process for 1 s or more (raise --history-concurrency or add processes)');
  if (headUnknown) reasons.push('room head unavailable: completeness unverified');
  const max = f => samples.reduce((m, s) => Math.max(m, s.generator[f] ?? 0), 0);
  const generator = { valid: !reasons.length, saturation_judged: judged, invalid_reasons: reasons, saturated_sample_share: saturatedShare, peak_cpu_cores: max('cpu_cores'), peak_rss_bytes: peakRss, peak_loop_delay_p99_ms: max('loop_delay_p99_ms'), peak_open_fds: peakFds, open_file_soft_limit: fdLimit === Infinity ? 'unlimited' : fdLimit, history_concurrency: historyConcurrency, history_queue_wait: historyWait.summary() };
  const phaseSummary = Object.fromEntries(phases.map(p => [p.name, { start_s: p.start_s, end_s: p.end_s, ready_at_end: p.ready_at_end, active_at_end: p.active_at_end, counters: p.counters, delivery_latency: p.delivery.summary(), reconnect_latency: p.reconnect.summary() }]));
  const result = { schema: 3, publish_failures_by_status: publishFailuresByStatus, unacknowledged_publishes: unacknowledged, acknowledged_positions: acknowledged, final_cursors: finalCursors, incomplete_clients: incomplete, date: new Date().toISOString(), commit, node: process.version, smoke: !!v.smoke, clients: initial, peak_clients: peak, joined_total: clients.length, left_total: counters.clients_left, targets: bases.length, payload_bytes: payloadBytes || null, ramp, duration_s: duration, rate_per_s: aggregateRate, publish_rate_per_publisher: perPublisherRate, publishers, ready_at_start: readyAtStart, ready_at_end: readyCount, active_at_end: activeCount, counters, delivery_latency: delivery.summary(), reconnect_latency: reconnect.summary(), phases: phaseSummary, stalled_readers: { count: stalledCount, stall_after_s: stallAfter, stall_for_s: stallFor, delivery_latency: stalledDelivery.summary(), reconnect_latency: stalledReconnect.summary(), catchup_after_resume: stalledCatchup.summary() }, topology, generator, samples };
  await writeFile(`${v.out}.partial`, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  await rename(`${v.out}.partial`, v.out);
  console.log(JSON.stringify({ counters, delivery_latency: result.delivery_latency, reconnect_latency: result.reconnect_latency, generator: { valid: generator.valid, invalid_reasons: reasons, peak_cpu_cores: generator.peak_cpu_cores, peak_rss_bytes: peakRss } }));
  if (counters.publish_unacknowledged || counters.void_numbers || counters.repeats || incomplete.length || !readyAtStart || (publishers && !counters.publish_ok) || (publishers && !delivery.count) || (!v.smoke && !generator.valid) || (v.smoke && (readyAtStart !== initial || counters.errors || counters.publish_failed))) process.exitCode = 1;
} finally {
  stopped = true; stop.abort(); loopDelay.disable(); for (const { client } of clients) client.close();
  await Promise.allSettled([...tasks, ...runTasks]);
}
