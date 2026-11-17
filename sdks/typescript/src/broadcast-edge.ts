import * as storage from "wasmkeep:actor/storage@0.2.0";
import * as host from "wasmkeep:actor/websockets@0.2.0";
import { trySend } from "./sockets";
import { Edge, type Push } from "./room-edge";
import type { ActorDefinition } from "./index";
interface Config {
    room: string;
    lo: string;
    hi?: string;
    state: "active" | "draining";
    cap?: number;
    e?: number;
    n?: number;
}
export function defineEdge(): ActorDefinition {
    const edge = new Edge({ send: trySend, close: (id, code) => host.close(id, code, code === 4801 ? "moved" : "slow") });
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
            if (!caller || caller === "outside" || caller === "front")
                return { status: 403 };
            load();
            if (req.method !== "POST")
                return { status: 405 };
            if (req.path === "/wasmkeep-edge/v1/end") {
                if (config && config.room !== caller)
                    return { status: 403 };
                const force = req.text(32) === 'force';
                if (config && (config.state !== 'draining' || (host.connections().length && !force)))
                    return { status: 409 };
                storage.deleteSelf();
                return { status: 200, body: '{}' };
            }
            if (["init", "range", "drain"].some(op => req.path === `/wasmkeep-edge/v1/${op}`)) {
                if (config && config.room !== caller)
                    return { status: 403 };
                let next: Config;
                try {
                    next = JSON.parse(req.text(4096) ?? "null");
                }
                catch {
                    return { status: 400 };
                }
                if (!next || next.room !== caller || typeof next.lo !== 'string' || !/^[0-9a-f]{16}$/.test(next.lo) || (next.hi !== undefined && (typeof next.hi !== 'string' || !/^[0-9a-f]{16}$/.test(next.hi) || next.hi <= next.lo)) || !['active', 'draining'].includes(next.state) || (next.cap !== undefined && (!Number.isInteger(next.cap) || next.cap < 1 || next.cap > 500)) || ((next.e !== undefined || next.n !== undefined) && ![next.e, next.n].every(n => Number.isSafeInteger(n) && n! >= 0)))
                    return { status: 400 };
                const init = req.path.endsWith('/init');
                if (!init && !config)
                    return { status: 409 };
                if (!init || !config) {
                    storage.execDurable("INSERT INTO wasmkeep_edge(id,config) VALUES(1,?1) ON CONFLICT(id) DO UPDATE SET config=excluded.config", [{ tag: "text", val: JSON.stringify(next) }]);
                    config = next;
                }
                if (!edge.head && config!.e !== undefined) {
                    edge.head = { e: config!.e, n: config!.n! };
                    edge.floor = config!.n!;
                }
                if (!init)
                    edge.range(BigInt('0x' + config!.lo), config!.hi === undefined ? undefined : BigInt('0x' + config!.hi), config!.state === 'draining', Array.from(host.connections()));
                return { status: 200, body: JSON.stringify({ sockets: host.connections().length }) };
            }
            if (req.path !== "/wasmkeep-edge/v1/push")
                return { status: 404 };
            if (config?.room !== caller)
                return { status: 403 };
            let p: Push;
            try {
                p = JSON.parse(req.text(1024 * 1024) ?? "null");
            }
            catch {
                return { status: 400 };
            }
            if (!p || (p.recover !== undefined && typeof p.recover !== "boolean") || ![p.e, p.b, p.first, p.head].every(n => Number.isSafeInteger(n) && n >= 0) || !Array.isArray(p.messages) || p.messages.length > 512 || p.first === 0 || p.b > p.head || (p.messages.length && p.first + p.messages.length - 1 > p.head))
                return { status: 400 };
            if (p.messages.some((m, i) => !m || !Number.isSafeInteger(m.a) || m.a < 0 || typeof m.d !== "string" || new TextEncoder().encode(m.d).length > 65536 || (m.i !== undefined && (typeof m.i !== "string" || new TextEncoder().encode(m.i).length > 64)) || ((m.e !== undefined || m.b !== undefined) && (!Number.isSafeInteger(m.e) || !Number.isSafeInteger(m.b) || m.e! < 0 || m.e! > p.e || m.b! < 0 || m.b! >= p.first + i))))
                return { status: 400 };
            if (!edge.head) {
                edge.head = { e: p.e, n: p.head };
                edge.floor = p.head;
            }
            edge.adopt(Array.from(host.connections()));
            const ack = edge.push(p);
            const result = { ...ack, opens, refusals: { unknown, moved, full } };
            opens = unknown = moved = full = 0;
            return { status: 200, body: JSON.stringify(result) };
        },
        websocket: {
            open(id, path) {
                load();
                if (!config) {
                    unknown++;
                    storage.deleteSelf();
                    throw Error("unknown");
                }
                // Match Rust wire::query_param without browser globals in the guest.
                const query = (name: string): string | null => {
                    for (const pair of (path.split("?")[1] ?? "").split("&")) {
                        const at = pair.indexOf("=");
                        if (at >= 0 && pair.slice(0, at) === name)
                            return pair.slice(at + 1);
                    }
                    return null;
                };
                const point = query("p");
                if (!point || !/^[0-9a-f]{16}$/.test(point) || !query("c") || config.state === "draining" || point < config.lo || (config.hi !== undefined && point >= config.hi)) {
                    moved++;
                    throw Error("moved");
                }
                const e = query("e"), n = query("n");
                if ((e === null) !== (n === null) || (e !== null && (!/^[0-9]+$/.test(e) || !/^[0-9]+$/.test(n!))))
                    throw Error("invalid position");
                const p = e !== null && n !== null ? { e: Number(e), n: Number(n) } : undefined;
                if (p && ![p.e, p.n].every(v => Number.isSafeInteger(v) && v >= 0))
                    throw Error("invalid position");
                edge.adopt(Array.from(host.connections()).filter(other => other !== id));
                if (host.connections().length > (config.cap ?? 500) || !edge.open(id, p)) {
                    full++;
                    throw Error("full");
                }
                edge.setPoint(id, BigInt("0x" + point));
                opens++;
                return true;
            },
            message(id, data) {
                if (typeof data === "string") {
                    try {
                        if (JSON.parse(data).t === "ping")
                            edge.ping(id);
                    }
                    catch { /* Unknown client frames have no effects. */ }
                }
            },
            close(id) { edge.forget(id); },
        },
    };
}
