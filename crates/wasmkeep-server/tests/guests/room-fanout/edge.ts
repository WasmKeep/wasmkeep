import { Edge } from '../../../../../sdks/typescript/src/room-edge';
import { trySend } from '../../../../../sdks/typescript/src/sockets';
import * as host from 'wasmkeep:actor/websockets@0.2.0';
const edge = new Edge({ send: trySend, close: (id, code) => host.close(id, code, 'slow') }, 500);
let n = 512;
export default {
    init() { edge.push({e:1,b:0,first:1,head:512,messages:Array.from({length:512},()=>({a:0,d:'x'.repeat(100)}))}); },
    fetch() { n++; return { status: 200, body: JSON.stringify(edge.push({e:1,b:0,first:n,head:n,messages:[{a:0,d:'new'}]})) }; },
    websocket: { open(id: bigint) { return edge.open(id); }, close(id: bigint) { edge.forget(id); } },
};
