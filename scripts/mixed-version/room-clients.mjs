// WebSocket room clients for scripts/mixed-version.sh: PER clients per node
// port, each joined to the test room through that node's front
// (crates/wasmkeep-server/tests/guests/room-scaling-front, the /r6/ routes).
// Each records every number it was handed, live or from the history: a live
// frame that skips ahead, a `behind`, or a `ready` (a join or rejoin) makes
// it fetch the history over HTTP, as the SDK's clients do; a closed socket
// makes it rejoin from where it stood. Every second it writes its state to
// STATE_DIR/<client>.json, which the script reads to check that every
// acknowledged publish reached every client (`have`). Repeats and new
// epochs (a room's cold start, ADR 0040, section 3) are counted, never an
// error. The test front reads the history at epoch 1 only, so after a new
// epoch it may refuse a catch-up: that is counted (`historyRefused`), and
// whether the numbers arrived anyway is what the script checks. An
// unexpected close code is recorded as a problem.
//
// Usage: node room-clients.mjs STATE_DIR ROOM PER PORT [PORT...]
// Stops on SIGTERM (or SIGINT), after writing its state one last time.
import { writeFileSync, renameSync } from "node:fs";

const [stateDir, room, per, ...ports] = process.argv.slice(2);
if (!stateDir || !room || !(Number(per) > 0) || ports.length === 0) {
  console.error("usage: room-clients.mjs STATE_DIR ROOM PER PORT [PORT...]");
  process.exit(2);
}
let stopping = false;

// The numbers after `from` (comma separated), or null when the front keeps
// refusing (503, or a trap on a cursor of another epoch).
async function history(port, from) {
  for (let attempt = 0; attempt < 6 && !stopping; attempt++) {
    try {
      const r = await fetch(`http://127.0.0.1:${port}/r6/history?room=${room}&n=${from}`, {
        signal: AbortSignal.timeout(10_000),
      });
      const body = await r.text();
      if (r.status === 200) return body === "" ? [] : body.split(",").map(Number);
    } catch {
      // A node restarting: retry.
    }
    await new Promise((res) => setTimeout(res, 500));
  }
  return null;
}

function client(port, k) {
  const id = `mv-${port}-${k}`;
  const s = {
    id, port, last: 0, frames: 0, live: 0, fetched: 0, fetches: 0, historyRefused: 0,
    repeats: 0, reconnects: 0, closes: {}, epochs: [], problems: [], connected: false,
  };
  const have = new Set();
  const take = (n, how) => {
    if (have.has(n)) { s.repeats++; return; }
    have.add(n);
    s[how]++;
    if (n > s.last) s.last = n;
  };
  let attempts = 0;
  // One chain per client, so frames and catch-ups apply in arrival order.
  let chain = Promise.resolve();
  const then = (f) => { chain = chain.then(f).catch((e) => s.problems.push(String(e))); };
  const fetchHistory = async () => {
    s.fetches++;
    const ns = await history(port, s.last);
    if (ns === null) { s.historyRefused++; return; }
    for (const n of ns) take(n, "fetched");
  };
  const connect = () => {
    if (stopping) return;
    const ws = new WebSocket(`ws://127.0.0.1:${port}/r6/join?room=${room}&c=${id}&n=${s.last}`);
    ws.onopen = () => { s.connected = true; attempts = 0; };
    ws.onmessage = (ev) => {
      s.frames++;
      let j;
      try { j = JSON.parse(String(ev.data)); } catch { s.problems.push(`not JSON: ${String(ev.data).slice(0, 80)}`); return; }
      if (j.t === "ready" || j.t === "behind") { then(fetchHistory); return; }
      if (j.t !== "m") return;
      if (!s.epochs.includes(j.e)) s.epochs.push(j.e);
      then(async () => {
        if (j.n > s.last + 1) await fetchHistory();
        const count = Array.isArray(j.m) ? j.m.length : 0;
        for (let n = j.n; n < j.n + count; n++) take(n, "live");
      });
    };
    ws.onclose = (ev) => {
      s.connected = false;
      s.closes[ev.code] = (s.closes[ev.code] ?? 0) + 1;
      // 1000 at our own stop; 1012 a drain or restart; 4801 a split; 1006 a
      // node killed, stopped or not answering yet. Anything else is worth a look.
      if (!stopping && ![1000, 1001, 1006, 1012, 4801].includes(ev.code)) {
        s.problems.push(`close ${ev.code} ${ev.reason}`);
      }
      if (stopping) return;
      s.reconnects++;
      attempts = Math.min(attempts + 1, 5);
      setTimeout(connect, 200 * 2 ** attempts + Math.floor(Math.random() * 200));
    };
    ws.onerror = () => {};
    s.ws = ws;
  };
  connect();
  return { s, have };
}

const clients = ports.flatMap((p) => Array.from({ length: Number(per) }, (_, k) => client(p, k)));
const save = () => {
  for (const { s, have } of clients) {
    const { ws, ...state } = s;
    state.have = [...have].sort((a, b) => a - b);
    const path = `${stateDir}/${s.id}.json`;
    writeFileSync(`${path}.tmp`, JSON.stringify(state));
    renameSync(`${path}.tmp`, path);
  }
};
const timer = setInterval(save, 1000);
const stop = () => {
  stopping = true;
  clearInterval(timer);
  for (const { s } of clients) try { s.ws?.close(1000); } catch {}
  save();
  setTimeout(() => process.exit(0), 200);
};
process.on("SIGTERM", stop);
process.on("SIGINT", stop);
