import { broadcast, defineFront } from '../../../../../sdks/typescript/src/front';
const room = new broadcast.Client('room/unicode');
export default defineFront({ fetch(req) {
        const result = req.path === '/publish'
            ? room.publish([{ id: 'x', body: 'plain' }, { id: '\uFEFFx', body: '\uFEFFbody' }])
            : broadcast.historyJSON(room.history({ e: 1n, n: 0n }));
        return { body: JSON.stringify(result, (_key, value) => typeof value === 'bigint' ? Number(value) : value) };
    } });
