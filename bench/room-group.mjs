#!/usr/bin/env node
// Remote ADR 0040 driver. No server lifecycle or benchmark result publication.
import { parseArgs } from 'node:util';
import { createRequire } from 'node:module';
import { execFileSync } from 'node:child_process';
import { writeFile, rename } from 'node:fs/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { randomUUID } from 'node:crypto';

const { values: v } = parseArgs({ options: {
  url: { type: 'string' }, clients: { type: 'string', default: '24' },
  duration: { type: 'string', default: '10' }, rate: { type: 'string', default: '2' },
  publishers: { type: 'string', default: '1' }, out: { type: 'string' },
  'metrics-url': { type: 'string', multiple: true, default: [] },
  'reconnect-every': { type: 'string', default: '0' }, smoke: { type: 'boolean' },
  help: { type: 'boolean' },
} });
if (v.help) {
  console.log('node bench/room-group.mjs --url https://front.example --clients 24 --duration 10 --rate 2 --publishers 1 --out result.json [--metrics-url URL ...] [--reconnect-every SECONDS] [--smoke]');
  process.exit(0);
}
function number(key, min, max, integer = false) {
  const n = Number(v[key]);
  if (!Number.isFinite(n) || n < min || n > max || (integer && !Number.isInteger(n))) throw Error(`invalid --${key}`);
  return n;
}
const count = number('clients', 1, 128000, true), duration = number('duration', 1, 86400);
const rate = number('rate', 0.01, 100000), publishers = number('publishers', 1, 1000, true);
const churn = number('reconnect-every', 0, 86400);
const base = new URL(v.url);
if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash) throw Error('URL must be an HTTP(S) front without credentials/query/fragment');
if (!v.out) throw Error('--out is required (use scratch storage; review before publication)');
if (v.smoke && (!['localhost', '127.0.0.1', '[::1]'].includes(base.hostname) || count > 32 || duration > 15 || rate > 5 || publishers > 2)) throw Error('smoke requires loopback, <=32 clients, <=15 seconds, <=5 messages/s, <=2 publishers');
const require = createRequire(new URL('../sdks/typescript/package.json', import.meta.url));
const { build } = require('esbuild');
const bundle = await build({ entryPoints: [new URL('../sdks/typescript/src/room-client.ts', import.meta.url).pathname], bundle: true, write: false, format: 'esm', platform: 'node' });
const { RoomClient } = await import(`data:text/javascript;base64,${Buffer.from(bundle.outputFiles[0].contents).toString('base64')}`);
// Logarithmic buckets: bounded memory; percentile upper bounds within 1% or 1ms.
class Histogram {
  bins = new Map(); count = 0;
  add(ms) { const b = Math.ceil(Math.log(Math.max(1, ms)) / Math.log(1.01)); this.bins.set(b, (this.bins.get(b) ?? 0) + 1); this.count++; }
  summary() { const entries = [...this.bins].sort((a, b) => a[0] - b[0]); return Object.fromEntries([['count', this.count], ...[50, 95, 99].map(p => { let n = 0; const found = entries.find(([, k]) => (n += k) >= Math.ceil(this.count * p / 100)); return [`p${p}_ms`, found ? 1.01 ** found[0] : null]; })]); }
}
const delivery = new Histogram(), reconnect = new Histogram();
const counters = { deliveries: 0, repeats: 0, history_requests: 0, gaps_recovered: 0, recovered_messages: 0, void_numbers: 0, resets: 0, reconnect_attempts: 0, close_4801: 0, close_4802: 0, errors: 0, publish_ok: 0, publish_failed: 0, publish_retries: 0, forced_disconnects: 0, smoke_dropped_frames: 0 };
const acknowledged = [];
const run = randomUUID().slice(0, 8), clients = [], sockets = new Set(), samples = [];
let stopped = false;
const stop = new AbortController();
const start = performance.now();
const endpoint = (path, name) => {
  const u = new URL(path, base.href.endsWith('/') ? base.href : base.href + '/');
  u.searchParams.set('name', name);
  return u;
};
const boundedFetch = (url, init = {}) => fetch(url, { ...init, signal: AbortSignal.any([stop.signal, AbortSignal.timeout(2000), ...(init.signal ? [init.signal] : [])]) });
for (let i = 0; i < count; i++) {
  const name = `b${run}-${i}`, recovered = new Set();
  let attempts = 0, down, ready = false, dropped = false;
  const join = endpoint('join', name); join.protocol = base.protocol === 'https:' ? 'wss:' : 'ws:';
  const client = new RoomClient({ join: join.href, history: endpoint('history', name).href,
    socket(url) {
      const ws = new WebSocket(url); sockets.add(ws);
      // One synthetic lost frame proves the HTTP recovery instrumentation.
      if (v.smoke && i === 0) ws.addEventListener('message', event => {
        if (!dropped && JSON.parse(event.data).t === 'm') {
          dropped = true; counters.smoke_dropped_frames++; event.stopImmediatePropagation();
        }
      });
      ws.addEventListener('close', e => { sockets.delete(ws); if (!stopped) { if (e.code === 4801) counters.close_4801++; if (e.code === 4802) counters.close_4802++; } });
      return ws;
    },
    async fetch(url, init) {
      counters.history_requests++;
      const response = await boundedFetch(url, init);
      if (!response.ok) return response;
      const h = await response.json();
      recovered.clear();
      for (const m of h.messages) recovered.add(`${m.position.e}.${m.position.n}`);
      if (h.messages.length && new URL(url).searchParams.has('after')) counters.gaps_recovered++;
      return { ok: true, status: response.status, json: async () => h };
    },
    onMessage(p, body) {
      counters.deliveries++;
      if (recovered.delete(`${p.e}.${p.n}`)) counters.recovered_messages++;
      // Timestamp originates on this load process, not a remote node's clock.
      const match = body.match(new RegExp(`wkbench:${run}:([0-9.]+):`));
      if (match) delivery.add(performance.now() - Number(match[1]));
    },
    onRepeat() { counters.repeats++; }, onLost(a, b) { counters.void_numbers += b - a + 1; },
    onReset() { counters.resets++; }, onError() { if (!stopped) counters.errors++; },
    onState(state) {
      ready = state === 'ready';
      if (state === 'connecting' && attempts++ > 0) counters.reconnect_attempts++;
      if (state === 'disconnected' && down === undefined) down = performance.now();
      if (state === 'ready' && down !== undefined) { reconnect.add(performance.now() - down); down = undefined; }
    },
  });
  clients.push({ client, ready: () => ready }); client.connect();
  // Bounded admission ramp, without blocking readers or catch-up bursts.
  await sleep(2);
}
let tasks = [];
try {
  const admissionDeadline = performance.now() + (v.smoke ? 3000 : 30000);
  while (clients.some(c => !c.ready()) && performance.now() < admissionDeadline) await sleep(50);
  const readyAtStart = clients.filter(c => c.ready()).length;
  const publishingStart = performance.now(), end = publishingStart + duration * 1000;
  tasks = Array.from({ length: publishers }, (_, publisher) => (async () => {
    let seq = 0;
    await sleep(Math.min(duration * 1000, publisher * 1000 / rate));
    while (!stopped && performance.now() < end) {
      const sent = performance.now(), id = `${run}-${publisher}-${seq++}`;
      const body = JSON.stringify({ id, text: `wkbench:${run}:${sent}:payload` });
      let ok = false;
      for (let k = 0; k < 3 && performance.now() < end && !stopped; k++) {
        try {
          const r = await boundedFetch(endpoint('publish', `p${run}-${publisher}`), { method: 'POST', headers: { 'content-type': 'application/json' }, body });
          if (r.ok) {
            const positions = await r.json();
            if (!Array.isArray(positions) || positions.length !== 1 || positions.some(p => !Number.isSafeInteger(p.e) || p.e < 1 || !Number.isSafeInteger(p.n) || p.n < 1)) throw Error('invalid publish acknowledgement');
            acknowledged.push(...positions);
            ok = true; break;
          }
          await r.arrayBuffer();
          if (![429, 503].includes(r.status)) break;
          const retry = r.headers.get('retry-after') ?? '0';
          const delay = Number.isFinite(Number(retry)) ? Number(retry) * 1000 : Math.max(0, Date.parse(retry) - Date.now());
          await sleep(Math.min(Math.max(0, end - performance.now()), (delay || 0) + Math.random() * 500));
        } catch { /* Unknown outcome: retry exactly the same id and body. */ }
        counters.publish_retries++;
        await sleep(Math.random() * 250);
      }
      counters[ok ? 'publish_ok' : 'publish_failed']++;
      await sleep(Math.min(Math.max(0, end - performance.now()), Math.max(0, publishers * 1000 / rate - (performance.now() - sent))));
    }
  })());
  let nextChurn = publishingStart + churn * 1000;
  while (performance.now() < end) {
    const sample = { elapsed_s: (performance.now() - start) / 1000, ready: clients.filter(c => c.ready()).length, edges: null, relays: null, nodes: [] };
    try { const r = await boundedFetch(endpoint('stats', `b${run}`)); if (!r.ok) throw Error(); const s = await r.json(); sample.edges = s.edges ?? null; sample.relays = s.relays ?? null; } catch { sample.stats_unavailable = true; }
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
    samples.push(sample);
    if (churn && performance.now() >= nextChurn) { const ws = [...sockets].find(s => s.readyState === WebSocket.OPEN); if (ws) { counters.forced_disconnects++; ws.close(); } nextChurn = performance.now() + churn * 1000; }
    await sleep(Math.min(1000, Math.max(0, end - performance.now())));
  }
  await Promise.all(tasks);
  const reached = p => p && acknowledged.every(a => p.e >= a.e && p.n >= a.n);
  const drainDeadline = performance.now() + (v.smoke ? 3000 : 10000);
  while (clients.some(c => !reached(c.client.last)) && performance.now() < drainDeadline) await sleep(50);
  const finalCursors = clients.map(c => c.client.last ?? null);
  const incomplete = finalCursors.flatMap((p, i) => reached(p) ? [] : [i]);
  let commit = 'unknown';
  try { commit = execFileSync('git', ['rev-parse', 'HEAD'], { encoding: 'utf8' }).trim(); if (execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }).trim()) commit += '-dirty'; } catch {}
  const result = { schema: 1, acknowledged_positions: acknowledged, final_cursors: finalCursors, incomplete_clients: incomplete, date: new Date().toISOString(), commit, node: process.version, smoke: !!v.smoke, clients: count, duration_s: duration, rate_per_s: rate, publishers, ready_at_start: readyAtStart, ready_at_end: clients.filter(c => c.ready()).length, counters, delivery_latency: delivery.summary(), reconnect_latency: reconnect.summary(), samples };
  await writeFile(`${v.out}.partial`, JSON.stringify(result, null, 2) + '\n', { flag: 'wx' });
  await rename(`${v.out}.partial`, v.out);
  console.log(JSON.stringify({ counters, delivery_latency: result.delivery_latency, reconnect_latency: result.reconnect_latency }));
  if (counters.void_numbers || incomplete.length || !readyAtStart || !counters.publish_ok || !delivery.count || (v.smoke && (readyAtStart !== count || counters.errors || counters.publish_failed))) process.exitCode = 1;
} finally {
  stopped = true; stop.abort(); for (const { client } of clients) client.close();
  await Promise.allSettled(tasks);
}
