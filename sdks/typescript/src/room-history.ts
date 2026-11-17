/** Pure JSON boundary shared by fronts and the browser; no host globals. */
import type { Position } from "./room-edge";
export interface History {
    head: Position;
    lost?: {
        e: number;
        base: number;
    };
    reset?: Position;
    messages: {
        position: Position;
        at: number;
        id?: string;
        body: string;
    }[];
    more: boolean;
}
/** Front adapter for R2's bigint history. JSON numbers are exact only up to
 * 2^53 - 1; reject larger values rather than silently changing identities. */
export function historyJSON(h: import("./broadcast-core").History): History {
    const number = (v: bigint) => { const n = Number(v); if ((!Number.isSafeInteger(n) || n < 0) || BigInt(n) !== v)
        throw Error("invalid browser integer"); return n; };
    const p = (v: import("./broadcast-core").Position) => ({ e: number(v.e), n: number(v.n) });
    return { head: p(h.head), ...(h.lost ? { lost: { e: number(h.lost.e), base: number(h.lost.base) } } : {}),
        ...(h.reset ? { reset: p(h.reset) } : {}), more: h.more,
        messages: h.messages.map(m => ({ position: p(m.position), at: number(m.at), ...(m.id === undefined ? {} : { id: m.id }), body: m.body })) };
}
