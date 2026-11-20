import { broadcast, defineFront } from '../../../../../sdks/typescript/src/front';
const room = new broadcast.Client('room/unicode');
/** Every code point class JSON escapes differently, as bodies and ids. */
function tricky() {
    const out: { id?: string; body: string }[] = [];
    for (let c = 0; c < 0x100; c += 16) {
        const body = Array.from({ length: 16 }, (_, i) => String.fromCharCode(c + i)).join('');
        out.push({ id: `t${c}:${body}`, body });
    }
    for (const body of ['"\\/', '  ', '﻿body', '\u{1F600}\u{10FFFF}', '€'.repeat(100)])
        out.push({ body });
    return out;
}
export default defineFront({ fetch(req) {
        // The room's JSON, forwarded unread; and the same page converted here, as fronts did.
        if (req.path === '/history')
            return { body: room.historyJSON({ e: 1n, n: 0n }) };
        const result = req.path === '/publish'
            ? room.publish([{ id: 'x', body: 'plain' }, { id: '﻿x', body: '﻿body' }])
            : req.path === '/publish-tricky'
                ? room.publish(tricky())
                : broadcast.historyJSON(room.history({ e: 1n, n: 0n }, 256));
        return { body: JSON.stringify(result, (_key, value) => typeof value === 'bigint' ? Number(value) : value) };
    } });
