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
}
const bytes = (s: string) => new TextEncoder().encode(s).length;
export class Edge {
    buffer: {
        n: number;
        e: number;
        b: number;
        item: Item;
    }[] = [];
    head?: Position;
    floor = 0;
    private cursors = new Map<bigint, Cursor>();
    private size = 0;
    private slow = 0;
    constructor(private io: SocketIO) { }
    open(id: bigint, position?: Position) {
        if (this.cursors.size >= 500)
            return false;
        const resume = this.head && position?.e === this.head.e && position.n >= this.floor && position.n <= this.head.n;
        const n = resume ? position!.n : this.head?.n ?? 0;
        this.cursors.set(id, { n, lagging: false });
        this.send(id, { t: "ready", e: this.head?.e ?? 0, n: !this.head || (position && !resume) ? null : n });
        this.deliver(id);
        return true;
    }
    forget(id: bigint) { this.cursors.delete(id); }
    /** Existing host sockets after a cold wake start at the next known head. */
    adopt(ids: bigint[]) { if (this.head)
        for (const id of ids)
            if (!this.cursors.has(id))
                this.cursors.set(id, { n: this.head.n, lagging: false }); }
    ping(id: bigint) { if (this.head)
        this.send(id, { t: "pong", ...this.head }); this.deliver(id); }
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
            this.size = this.buffer.reduce((n, m) => n + bytes(JSON.stringify(m.item)), 0);
            this.head = { e: p.e, n: Math.min(this.head.n, p.b) };
            this.floor = Math.min(this.floor, p.b);
            for (const c of this.cursors.values())
                c.n = Math.min(c.n, p.b);
        }
        if (p.recover) {
            this.buffer = []; this.size = 0; this.floor = p.head; this.head.n = p.head;
        }
        if (p.messages.length && p.first > this.head.n + 1) {
            this.buffer = [];
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
            this.buffer.push({ n, e: item.e ?? p.e, b: item.b ?? p.b, item: payload });
            this.size += bytes(JSON.stringify(payload));
            this.head.n = n;
            while (this.buffer.length > 512 || this.size > 1024 * 1024) {
                const old = this.buffer.shift()!;
                this.floor = old.n;
                this.size -= bytes(JSON.stringify(old.item));
            }
        }
        for (const id of this.cursors.keys())
            this.deliver(id);
        return this.answer();
    }
    private behind(id: bigint, c: Cursor) {
        const result = this.send(id, { t: "behind", ...this.head });
        if (result === "backpressure") {
            this.io.close(id, 4802);
            this.slow++;
            this.forget(id);
        }
        else if (result === "gone")
            this.forget(id);
        else {
            c.n = this.head!.n;
            c.lagging = false;
        }
    }
    private deliver(id: bigint) {
        const c = this.cursors.get(id);
        if (!c || !this.head)
            return;
        if (c.n < this.floor) {
            this.behind(id, c);
            return;
        }
        const pending = this.buffer.filter(m => m.n > c.n);
        if (!pending.length)
            return;
        const first = pending[0], items: Item[] = [];
        let frame = "";
        for (const m of pending) {
            if (m.e !== first.e)
                break;
            const next = JSON.stringify({ t: "m", e: first.e, b: first.b, n: first.n, m: [...items, m.item] });
            if (bytes(next) > 65536)
                break;
            items.push(m.item);
            frame = next;
        }
        // A single maximum-size text can exceed the JSON frame bound: recover it
        // through history, which already supports 256 KiB responses.
        if (!items.length) {
            this.behind(id, c);
            return;
        }
        const result = this.io.send(id, frame);
        if (result === "ok") {
            c.n = first.n + items.length - 1;
            c.lagging = c.n < this.head.n;
        }
        else if (result === "gone")
            this.forget(id);
        else
            c.lagging = true;
    }
    private answer() {
        const answer = { ...this.head!, sockets: this.cursors.size, lagging: [...this.cursors.values()].filter(c => c.lagging).length, slow: this.slow };
        this.slow = 0;
        return answer;
    }
}
