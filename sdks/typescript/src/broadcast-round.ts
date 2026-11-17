import * as calls from "wasmkeep:actor/calls@0.2.0";
import * as actors from "wasmkeep:actor/actors@0.2.0";
import * as alarms from "wasmkeep:actor/alarms@0.2.0";
import type { Core, Position } from "./broadcast-core";
import type { Push } from "./room-edge";
interface Child { address: string; position?: Position; initialized: boolean; failures: number; due: number }
export class Round {
  private children: Child[];
  constructor(addresses: string[]) {
    if (addresses.length > 16) throw Error("at most 16 fixed edges");
    this.children = addresses.map(address => ({ address, initialized: false, failures: 0, due: 0 }));
  }
  run(core: Core, now: number) {
    if (!core.beforePush(BigInt(now))) return;
    const epoch = core.wake(), rows = core.messages(), epochs = core.epochs(), head = rows.at(-1)?.position.n ?? epoch.base;
    const due = this.children.filter(c => c.due <= now), enc = new TextEncoder();
    const requests: [string, actors.Request][] = due.map(c => {
      let body: string, op: string;
      if (!c.initialized) {
        const a = actors.selfAddress(); op = "init";
        body = JSON.stringify({ room: `${a.tenant}/${a.class}/${a.name}`, lo: "0000000000000000", state: "active" });
      } else {
        op = "push";
        const p: Push = { e: Number(epoch.e), b: Number(epoch.base), first: Number(head + 1n), head: Number(head), messages: [] };
        if (c.position) {
          const n = epochs.filter(e => e.e > c.position!.e).reduce((n, e) => n < e.base ? n : e.base, c.position.n);
          for (const m of rows.filter(m => m.position.n > n)) {
            if (!p.messages.length) p.first = Number(m.position.n);
            p.messages.push({ a: Number(m.at), i: m.id, d: m.body });
            if (p.messages.length > 512 || enc.encode(JSON.stringify(p)).length > 512 * 1024) { p.messages.pop(); break; }
          }
        }
        body = JSON.stringify(p);
      }
      return [c.address, { method: "POST", path: `/wasmkeep-edge/v1/${op}`, headers: [], body: enc.encode(body) }];
    });
    let next = Infinity;
    const results = calls.callMany(requests);
    due.forEach((c, i) => {
      try {
        const r = results[i]; if (r.tag !== "ok" || r.val.status !== 200) throw Error("failed push");
        if (!c.initialized) { c.initialized = true; next = Math.min(next, now + 20); }
        else {
          const a = JSON.parse(new TextDecoder().decode(r.val.body));
          if (![a.e, a.n, a.lagging].every(n => Number.isSafeInteger(n) && n >= 0) || BigInt(a.e) > epoch.e || BigInt(a.n) > head) throw Error("invalid acknowledgment");
          c.position = { e: BigInt(a.e), n: BigInt(a.n) };
          if (c.position.e !== epoch.e || c.position.n < head || a.lagging) next = Math.min(next, now + 20);
        }
        c.failures = 0; c.due = 0;
      } catch { c.due = now + Math.min(16000, 2000 * 2 ** Math.min(c.failures++, 3)); }
    });
    for (const c of this.children) if (c.due > now) next = Math.min(next, c.due);
    if (next !== Infinity) alarms.setAlarm(BigInt(next));
  }
}
