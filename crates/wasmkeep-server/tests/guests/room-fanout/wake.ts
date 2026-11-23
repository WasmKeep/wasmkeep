// An edge with no room: each request is the first push after a wake
// (`.../header` header-only, otherwise one message) to the sockets kept.
import { Edge } from '../../../../../sdks/typescript/src/room-edge';
import { trySend } from '../../../../../sdks/typescript/src/sockets';
import * as host from 'wasmkeep:actor/websockets@0.2.0';
const edge = new Edge({ send: trySend, close: (id, code) => host.close(id, code, 'slow') }, 500);
export default {
    fetch(req: { path: string }) {
        const header = req.path.endsWith('header');
        const p = header
            ? { e: 1, b: 0, first: 513, head: 512, messages: [] }
            : { e: 1, b: 0, first: 513, head: 513, messages: [{ a: 0, d: 'new' }] };
        return { status: 200, body: JSON.stringify(edge.receive(p, Array.from(host.connections()))) };
    },
    websocket: { open(id: bigint) { return edge.open(id); }, close(id: bigint) { edge.forget(id); } },
};
