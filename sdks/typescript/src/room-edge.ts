/** In-memory delivery state. No storage, calls, timers, or second socket queue. */
export interface Position {
    e: number;
    n: number;
}
export interface Item {
    a: number;
    i?: string;
    d: string;
    e?: number;
    b?: number;
}
export interface Push {
    recover?: boolean;
    e: number;
    b: number;
    first: number;
    head: number;
    messages: Item[];
}
export type SendResult = "ok" | "gone" | "backpressure";
export interface SocketIO {
    send(id: bigint, frame: string): SendResult;
    close(id: bigint, code: number): void;
}
interface Cursor {
    n: number;
    lagging: boolean;
    stalled?: boolean;
}
import { byteLength as bytes } from "./room-utf8";
export class Edge {
    buffer: {
        n: number;
        e: number;
        b: number;
        json: string;
        bytes: number;
    }[] = [];
    head?: Position;
    floor = 0;
    private cursors = new Map<bigint, Cursor>();
    private index = new Map<number, (typeof this.buffer)[number]>();
    private size = 0;
    private slow = 0;
    constructor(private io: SocketIO, readonly capacity = 500) { }
    private points = new Map<bigint, bigint>();
    setPoint(id: bigint, point: bigint) { this.points.set(id, point); }
    range(lo: bigint, hi: bigint | undefined, drain: boolean, ids: bigint[]) {
        for (const id of ids) {
            const p = this.points.get(id);
            if (drain || p === undefined || p < lo || (hi !== undefined && p >= hi)) {
                this.io.close(id, 4801);
                this.forget(id);
            }
        }
    }
    open(id: bigint, position?: Position) {
        if (this.cursors.size >= this.capacity)
            return false;
        const resume = this.head && position?.e === this.head.e && position.n >= this.floor && position.n <= this.head.n;
        const n = resume ? position!.n : this.head?.n ?? 0;
        this.cursors.set(id, { n, lagging: false });
        this.send(id, { t: "ready", e: this.head?.e ?? 0, n: !this.head || (position && !resume) ? null : n });
        this.deliver(id);
        return true;
    }
    forget(id: bigint) { this.cursors.delete(id); this.points.delete(id); }
    /** Existing host sockets after a cold wake start at the next known head. */
    adopt(ids: bigint[]) {
        if (this.head)
            for (const id of ids)
                if (!this.cursors.has(id))
                    this.cursors.set(id, { n: this.head.n, lagging: false });
    }
    ping(id: bigint) {
        if (this.head)
            this.send(id, { t: "pong", ...this.head });
        this.deliver(id);
    }
    private send(id: bigint, frame: unknown) { return this.io.send(id, JSON.stringify(frame)); }
    push(p: Push) {
        if (this.head && p.e < this.head.e)
            return this.answer();
        if (!this.head) {
            this.head = { e: p.e, n: p.messages.length ? p.first - 1 : p.head };
            this.floor = this.head.n;
        }
        if (p.e > this.head.e) {
            this.buffer = this.buffer.filter(m => m.n <= p.b);
            this.size = this.buffer.reduce((n, m) => n + m.bytes, 0);
            this.index = new Map(this.buffer.map(m => [m.n, m]));
            this.head = { e: p.e, n: Math.min(this.head.n, p.b) };
            this.floor = Math.min(this.floor, p.b);
            for (const c of this.cursors.values())
                c.n = Math.min(c.n, p.b);
        }
        if (p.recover) {
            this.buffer = [];
            this.index.clear();
            this.size = 0;
            this.floor = p.head;
            this.head.n = p.head;
        }
        if (p.messages.length && p.first > this.head.n + 1) {
            this.buffer = [];
            this.index.clear();
            this.size = 0;
            this.floor = p.first - 1;
            this.head.n = this.floor;
        }
        for (let i = 0; i < p.messages.length; i++) {
            const n = p.first + i;
            if (n <= this.head.n)
                continue;
            const item = p.messages[i];
            const payload = { a: item.a, i: item.i, d: item.d };
            const json = JSON.stringify(payload);
            const stored = { n, e: item.e ?? p.e, b: item.b ?? p.b, json, bytes: bytes(json) };
            this.buffer.push(stored);
            this.index.set(n, stored);
            this.size += stored.bytes;
            this.head.n = n;
            while (this.buffer.length > 512 || this.size > 1024 * 1024) {
                const old = this.buffer.shift()!;
                this.floor = old.n;
                this.size -= old.bytes;
                this.index.delete(old.n);
            }
        }
        this.deliverMany([...this.cursors.keys()]);
        return this.answer();
    }
    private deliver(id: bigint) { this.deliverMany([id]); }
    private deliverMany(ids: bigint[]) {
        if (!this.head) return;
        const groups = new Map<number, bigint[]>();
        for (const id of ids) {
            const c = this.cursors.get(id);
            if (!c || c.n >= this.head.n) continue;
            const group = groups.get(c.n) ?? [];
            group.push(id); groups.set(c.n, group);
        }
        let frames = 0, frameBytes = 0;
        // Highest cursor first: the group caught up before this push leads.
        for (const [n, group] of [...groups].sort((a, b) => b[0] - a[0])) {
            if (frames >= 16 || frameBytes >= 1024 * 1024) {
                for (const id of group) { const c = this.cursors.get(id)!; c.lagging = true; c.stalled = false; }
                continue;
            }
            const first = this.index.get(n + 1);
            let behind = n < this.floor || !first;
            let frame = '', last = n, length = 0;
            if (!behind && first) {
                const prefix = `{"t":"m","e":${first.e},"b":${first.b},"n":${first.n},"m":[`;
                const items: string[] = [];
                length = bytes(prefix) + 2;
                for (let at = n + 1; at <= this.head.n; at++) {
                    const m = this.index.get(at);
                    if (!m || m.e !== first.e || length + m.bytes + (items.length ? 1 : 0) > 65536) break;
                    length += m.bytes + (items.length ? 1 : 0);
                    items.push(m.json); last = at;
                }
                behind = !items.length;
                if (!behind) frame = prefix + items.join(',') + ']}';
            }
            if (behind) { frame = JSON.stringify({t:'behind', ...this.head}); length = bytes(frame); last = this.head.n; }
            // Each frame is <=64 KiB, so 16 frames also bound bytes to 1 MiB.
            frames++; frameBytes += length;
            for (const id of group) {
                const c = this.cursors.get(id)!;
                const result = this.io.send(id, frame);
                if (result === 'gone') this.forget(id);
                else if (result === 'backpressure' && behind) {
                    this.io.close(id, 4802); this.slow++; this.forget(id);
                } else {
                    if (result === 'ok') c.n = last;
                    c.lagging = c.n < this.head.n;
                    c.stalled = result === 'backpressure';
                }
            }
        }
    }
    private answer() {
        const answer = { ...this.head!, sockets: this.cursors.size, lagging: [...this.cursors.values()].filter(c => c.lagging).length, stalled: [...this.cursors.values()].filter(c => c.lagging && c.stalled).length, slow: this.slow };
        this.slow = 0;
        return answer;
    }
}
