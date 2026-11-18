import * as calls from 'wasmkeep:actor/calls@0.2.0';
import * as storage from 'wasmkeep:actor/storage@0.2.0';
import type { ActorDefinition } from './index';
import { unpackRelay } from './room-relay';
import { decode, encode } from './room-utf8';
/** Stateless, downward-only fan-out. Deploy as an internal class. */
export function defineRelay(): ActorDefinition {
    return { fetch(req) {
            const caller = req.headers['x-wasmkeep-caller'];
            if (!caller || caller === 'outside' || caller === 'front')
                return { status: 403 };
            if (req.method !== 'POST')
                return { status: 405 };
            if (req.path === '/wasmkeep-relay/v1/init')
                return { status: 200, body: '{}' };
            if (req.path === '/wasmkeep-relay/v1/end') {
                storage.deleteSelf();
                return { status: 200, body: '{}' };
            }
            if (req.path !== '/wasmkeep-relay/v1/push')
                return { status: 404 };
            let children;
            try {
                const body = req.bytes(1024 * 1024);
                children = unpackRelay(JSON.parse(body === undefined ? 'null' : decode(body, { stripBOM: true })));
            }
            catch {
                return { status: 400 };
            }
            const dec = new TextDecoder();
            const results = calls.callMany(children.map(c => [c.edge, { method: 'POST', path: '/wasmkeep-edge/v1/push', headers: [], body: encode(JSON.stringify(c.push)) }]));
            return { status: 200, body: JSON.stringify(results.map(r => {
                    try {
                        return r.tag === 'ok' && r.val.status === 200 ? JSON.parse(dec.decode(r.val.body)) : null;
                    }
                    catch {
                        return null;
                    }
                })) };
        } };
}
