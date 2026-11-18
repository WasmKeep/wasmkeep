// A test guest for crates/wasmkeep-server/tests/big_room.rs: an edge class
// that never moves. Each answers a push with its own fixed position (the
// number ending its name, times 7, at most the head), so every round of
// the room finds as many lagging children, at as many distinct positions,
// as it has.
import { actors, defineActor } from "@wasmkeep/sdk";

export default defineActor({
  fetch(req) {
    if (!req.path.startsWith("/wasmkeep-edge/v1/")) return { status: 404 };
    if (!req.path.endsWith("/push")) return '{"sockets":0}';
    const p = JSON.parse(req.text()) as { e: number; head: number };
    const at = Number(/(\d+)$/.exec(actors.self().name)?.[1] ?? "0") * 7;
    return JSON.stringify({ e: p.e, n: Math.min(at, p.head), lagging: 0, sockets: 0 });
  },
});
