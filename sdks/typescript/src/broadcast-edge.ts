import * as storage from "wasmkeep:actor/storage@0.2.0";
import * as host from "wasmkeep:actor/websockets@0.2.0";
import { trySend } from "./sockets";
import { Edge, type Push } from "./room-edge";
import type { ActorDefinition } from "./index";
interface Config { room: string; lo: string; hi?: string; state: "active" }
export function defineEdge(): ActorDefinition {
  const edge = new Edge({ send: trySend, close: (id, code) => host.close(id, code, "slow") });
  let config: Config | undefined;
  let opens = 0, unknown = 0, moved = 0, full = 0;
  const load = () => {
    const v = storage.query("SELECT config FROM wasmkeep_edge WHERE id=1", []).values[0]?.[0];
    config = v?.tag === "text" ? JSON.parse(v.val) as Config : undefined;
  };
  return {
    init() { storage.exec("CREATE TABLE IF NOT EXISTS wasmkeep_edge(id INTEGER PRIMARY KEY CHECK(id=1),config TEXT NOT NULL)", []); },
    fetch(req) {
      const caller = req.headers["x-wasmkeep-caller"];
      if (!caller || caller === "outside" || caller === "front") return { status: 403 };
      load();
      if (req.method !== "POST") return { status: 405 };
      if (req.path === "/wasmkeep-edge/v1/init") {
        if (config && config.room !== caller) return { status: 403 };
        const next = JSON.parse(req.text(4096) ?? "null") as Config;
        if (!next || next.room !== caller || next.lo !== "0000000000000000" || next.hi !== undefined || next.state !== "active") return { status: 400 };
        if (!config) storage.execDurable("INSERT INTO wasmkeep_edge(id,config) VALUES(1,?1)", [{ tag: "text", val: JSON.stringify(next) }]);
        config = next;
        return { status: 200, body: "{}" };
      }
      if (req.path !== "/wasmkeep-edge/v1/push") return { status: 404 };
      if (config?.room !== caller) return { status: 403 };
      const p = JSON.parse(req.text(1024 * 1024) ?? "null") as Push;
      if (!p || ![p.e,p.b,p.first,p.head].every(n => Number.isSafeInteger(n) && n >= 0) || !Array.isArray(p.messages) || p.messages.length > 512 || p.first === 0 || p.b > p.head || (p.messages.length && p.first + p.messages.length - 1 > p.head)) return { status: 400 };
      const ack = edge.push(p);
      edge.adopt(Array.from(host.connections()));
      const result = { ...ack, opens, refusals: { unknown, moved, full } }; opens = unknown = moved = full = 0;
      return { status: 200, body: JSON.stringify(result) };
    },
    websocket: {
      open(id, path) {
        load();
        if (!config) { unknown++; storage.deleteSelf(); throw Error("unknown"); }
        const q = new URL(path, "https://edge.invalid").searchParams, point = q.get("p");
        if (!point || !/^[0-9a-f]{16}$/.test(point) || !q.get("c")) { moved++; throw Error("moved"); }
        const e = q.get("e"), n = q.get("n");
        const p = e !== null && n !== null ? { e: Number(e), n: Number(n) } : undefined;
        if (p && ![p.e,p.n].every(v => Number.isSafeInteger(v) && v >= 0)) throw Error("invalid position");
        edge.adopt(Array.from(host.connections()).filter(other => other !== id));
        if (host.connections().length > 500 || !edge.open(id, p)) { full++; throw Error("full"); }
        opens++; return true;
      },
      message(id, data) { if (typeof data === "string") { try { if (JSON.parse(data).t === "ping") edge.ping(id); } catch { /* Unknown client frames have no effects. */ } } },
      close(id) { edge.forget(id); },
    },
  };
}
