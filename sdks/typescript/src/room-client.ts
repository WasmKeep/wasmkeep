/** Browser-only room transport: no WASI, actor, or Node imports. */
import type { Position, Item } from "./room-edge";
export type { Position } from "./room-edge";
import type { History } from "./room-history";
export { historyJSON, type History } from "./room-history";
export type Frame = {
    t: "m";
    e: number;
    b: number;
    n: number;
    m: Item[];
} | {
    t: "ready";
    e: number;
    /** Set when an edge woken from hibernation announces where its stream
     * starts to a socket already open: the epoch's base, as in an `m` frame. */
    b?: number;
    n: number | null;
} | {
    t: "behind";
    e: number;
    n: number;
} | {
    /** `n: null`: the edge knows no head yet (a keepalive only). */
    t: "pong";
    e: number;
    n: number | null;
};
export interface ReceiverOptions {
    history(after?: Position, signal?: AbortSignal): Promise<History>;
    onMessage?(p: Position, body: string, id?: string, at?: number): void;
    /** An item discarded because its position was already applied. */
    onRepeat?(p: Position): void;
    onLost?(first: number, last: number): void;
    onReset?(p: Position): void;
}
const integer = (n: unknown): n is number => typeof n === "number" && Number.isSafeInteger(n) && n >= 0;
const position = (p: any): p is Position => p && integer(p.e) && integer(p.n);
function validateFrame(f: any): asserts f is Frame {
    if (!f || !integer(f.e))
        throw Error("invalid frame");
    if (f.t === "m") {
        if (!integer(f.b) || !integer(f.n) || f.n === 0 || !Array.isArray(f.m) || !f.m.length ||
            !integer(f.n + f.m.length - 1) || f.m.some((m: any) => !m || !integer(m.a) || typeof m.d !== "string" || (m.i !== undefined && typeof m.i !== "string")))
            throw Error("invalid message frame");
    }
    else if (!["ready", "behind", "pong"].includes(f.t) || !(integer(f.n) || (f.t !== "behind" && f.n === null)) ||
        (f.t === "ready" && f.b !== undefined && (!integer(f.b) || f.n === null)))
        throw Error("invalid control frame");
}
function validateHistory(h: any): asserts h is History {
    if (!h || !position(h.head) || typeof h.more !== "boolean" || !Array.isArray(h.messages) ||
        (h.lost !== undefined && (!integer(h.lost?.e) || !integer(h.lost?.base))) ||
        (h.reset !== undefined && !position(h.reset)) ||
        h.messages.some((m: any) => !m || !position(m.position) || !integer(m.at) || typeof m.body !== "string" || (m.id !== undefined && typeof m.id !== "string")))
        throw Error("invalid history");
}
/** The rows of a full history page, as the front serves it by default. */
export const HISTORY_PAGE = 256;
export function backoff(k: number, random = Math.random, closeCode?: number): number {
    return random() * (k === 0 && (closeCode === 4801 || closeCode === 1012) ? 2000 : Math.min(30000, 500 * 2 ** Math.min(k, 16)));
}
export function joinURL(url: string, last?: Position): string {
    const u = new URL(url);
    if (last) {
        u.searchParams.set("e", String(last.e));
        u.searchParams.set("n", String(last.n));
    }
    return u.toString();
}
/** Serializes live frames behind gap recovery, including multi-page history. */
export class Receiver {
    last?: Position;
    private work?: Promise<void>;
    private queue: {
        frame: Frame;
        bytes: number;
    }[] = [];
    private bytes = 0;
    private abort = new AbortController();
    /** Includes the in-flight frame. Strings are conservatively charged as UTF-16. */
    get pending() { return { frames: this.queue.length, bytes: this.bytes }; }
    constructor(private options: ReceiverOptions, last?: Position) { this.last = last; }
    cancel() {
        this.abort.abort();
        this.abort = new AbortController();
        this.queue = [];
        this.bytes = 0;
        this.work = undefined;
    }
    receive(frame: Frame): Promise<void> {
        validateFrame(frame);
        const bytes = 128 + (frame.t === "m" ? frame.m.reduce((n, m) => n + 64 + 2 * (m.d.length + (m.i?.length ?? 0)), 0) : 0);
        if (this.queue.length >= 64 || this.bytes + bytes > 1024 * 1024) {
            this.cancel();
            throw Error("live backlog overflow");
        }
        this.queue.push({ frame, bytes });
        this.bytes += bytes;
        if (!this.work) {
            const signal = this.abort.signal;
            this.work = Promise.resolve().then(async () => {
                try {
                    while (this.queue.length) {
                        signal.throwIfAborted();
                        const item = this.queue[0];
                        await this.apply(item.frame, signal);
                        signal.throwIfAborted();
                        this.queue.shift();
                        this.bytes -= item.bytes;
                    }
                }
                catch (error) {
                    if (!signal.aborted)
                        this.cancel();
                    throw error;
                }
                finally {
                    if (!signal.aborted)
                        this.work = undefined;
                }
            });
        }
        return this.work;
    }
    private message(p: Position, body: string, id?: string, at?: number) {
        if (this.last && (p.e < this.last.e || (p.e === this.last.e && p.n <= this.last.n))) {
            this.options.onRepeat?.(p);
            return;
        }
        this.options.onMessage?.(p, body, id, at);
        this.last = p;
    }
    private async recover(signal: AbortSignal) {
        for (;;) {
            const before = this.last;
            const h = await this.options.history(this.last, signal);
            signal.throwIfAborted();
            validateHistory(h);
            if (h.lost) {
                if (this.last && h.lost.base < this.last.n)
                    this.options.onLost?.(h.lost.base + 1, this.last.n);
                this.last = { e: h.lost.e, n: h.lost.base };
            }
            if (h.reset) {
                this.options.onReset?.(h.reset);
                this.last = { e: h.reset.e, n: h.reset.n - 1 };
            }
            for (const m of h.messages) {
                // HTTP history is authoritative; a current-epoch head can precede
                // retained rows whose original identity belongs to an older epoch.
                if (this.last && m.position.n <= this.last.n) {
                    this.options.onRepeat?.(m.position);
                    continue;
                }
                if (this.last && m.position.n !== this.last.n + 1)
                    throw Error("invalid history gap");
                this.options.onMessage?.(m.position, m.body, m.id, m.at);
                this.last = m.position;
            }
            if (h.more && before?.e === this.last?.e && before?.n === this.last?.n)
                throw Error("invalid history progress");
            if (!h.more) {
                // A clean epoch change with no new messages still advances the epoch.
                if (!this.last || h.head.n === this.last.n)
                    this.last = h.head;
                return;
            }
        }
    }
    private async apply(f: Frame, signal: AbortSignal) {
        signal.throwIfAborted();
        if (f.t === "ready" && !this.last && f.n !== null) {
            this.last = { e: f.e, n: f.n };
            return;
        }
        // A pong with no head (an edge woken with no push yet) is a keepalive.
        if (f.t === "pong" && f.n === null)
            return;
        // A woken edge announces where its stream starts (ADR 0040, section 5):
        // the next epoch's base applies as in rule 2, and only a position past
        // the client's own is a gap.
        if (f.t === "ready" && f.b !== undefined && f.n !== null && this.last) {
            if (f.e === this.last.e + 1) {
                if (f.b < this.last.n) {
                    this.options.onLost?.(f.b + 1, this.last.n);
                    this.last = { e: f.e, n: f.b };
                }
                else
                    this.last = { e: f.e, n: this.last.n };
            }
            if (f.e !== this.last.e || f.n > this.last.n)
                await this.recover(signal);
            signal.throwIfAborted();
            return;
        }
        if (f.t !== "m") {
            if (f.t === "behind" || f.n === null || !this.last || f.e !== this.last.e || f.n !== this.last.n)
                await this.recover(signal);
            signal.throwIfAborted();
            return;
        }
        if (!this.last || f.e > this.last.e + 1)
            await this.recover(signal);
        signal.throwIfAborted();
        if (this.last && f.e === this.last.e + 1) {
            if (f.b < this.last.n) {
                this.options.onLost?.(f.b + 1, this.last.n);
                this.last = { e: f.e, n: f.b };
            }
            else
                this.last = { e: f.e, n: this.last.n };
        }
        if (this.last && f.e < this.last.e) {
            for (let i = 0; i < f.m.length && f.n + i <= this.last.n; i++)
                this.options.onRepeat?.({ e: f.e, n: f.n + i });
            if (f.n + f.m.length - 1 > this.last.n)
                await this.recover(signal);
            signal.throwIfAborted();
            return;
        }
        if (!this.last || f.n > this.last.n + 1)
            await this.recover(signal);
        signal.throwIfAborted();
        for (let i = 0; i < f.m.length; i++) {
            const m = f.m[i], p = { e: f.e, n: f.n + i };
            if (this.last && p.e === this.last.e && p.n > this.last.n + 1)
                throw Error("history did not fill gap");
            this.message(p, m.d, m.i, m.a);
        }
    }
}
export interface Clock {
    now(): number;
    set(fn: () => void, ms: number): unknown;
    clear(id: unknown): void;
}
export interface Socket {
    onopen: (() => void) | null;
    onmessage: ((event: {
        data: string;
    }) => void) | null;
    onclose: ((event: {
        code: number;
    }) => void) | null;
    onerror: (() => void) | null;
    send(s: string): void;
    close(): void;
}
export interface ClientOptions extends Omit<ReceiverOptions, "history"> {
    join: string;
    history: string;
    last?: Position;
    random?: () => number;
    clock?: Clock;
    socket?: (url: string) => Socket;
    fetch?: typeof fetch;
    onState?: (state: "connecting" | "ready" | "disconnected" | "closed") => void;
    onError?: (error: unknown) => void;
}
/** Call connect() to start and close() to cancel transport and pending retries. */
export class RoomClient {
    readonly receiver: Receiver;
    private clock: Clock;
    private random: () => number;
    private socket?: Socket;
    private timers = new Set<unknown>();
    private heartbeat?: unknown;
    private stable?: unknown;
    private stopped = true;
    private attempt = 0;
    private generation = 0;
    private abort = new AbortController();
    constructor(private options: ClientOptions) {
        this.clock = options.clock ?? { now: Date.now, set: (fn, ms) => setTimeout(fn, ms), clear: id => clearTimeout(id as ReturnType<typeof setTimeout>) };
        this.random = options.random ?? Math.random;
        this.receiver = new Receiver({ ...options, history: (p, signal) => this.history(p, signal) }, options.last);
    }
    get last() { return this.receiver.last; }
    private later(fn: () => void, ms: number) {
        const id = this.clock.set(() => { this.timers.delete(id); fn(); }, ms);
        this.timers.add(id);
        return id;
    }
    private cancel(id: unknown) { this.clock.clear(id); this.timers.delete(id); }
    connect() {
        if (!this.stopped)
            return;
        this.stopped = false;
        this.abort = new AbortController();
        this.open();
    }
    close() {
        this.stopped = true;
        this.generation++;
        this.abort.abort();
        this.receiver.cancel();
        for (const id of this.timers)
            this.clock.clear(id);
        this.timers.clear();
        this.socket?.close();
        this.options.onState?.("closed");
    }
    private open() {
        if (this.stopped)
            return;
        const generation = ++this.generation;
        this.options.onState?.("connecting");
        let ended = false;
        const disconnected = (code: number) => {
            if (ended || generation !== this.generation || this.stopped)
                return;
            ended = true;
            this.receiver.cancel();
            this.cancel(this.heartbeat);
            this.cancel(this.stable);
            this.options.onState?.("disconnected");
            this.later(() => this.open(), backoff(this.attempt++, this.random, code));
        };
        try {
            const ws = this.socket = (this.options.socket ?? (url => new WebSocket(url) as unknown as Socket))(joinURL(this.options.join, this.last));
            const keepalive = () => {
                this.cancel(this.heartbeat);
                this.heartbeat = this.later(() => {
                    try {
                        ws.send('{"t":"ping"}');
                    }
                    catch {
                        disconnected(1006);
                        ws.close();
                        return;
                    }
                    this.heartbeat = this.later(() => { disconnected(1006); ws.close(); }, 10000);
                }, 45000);
            };
            ws.onopen = () => {
                if (!ended && generation === this.generation && !this.stopped)
                    keepalive();
            };
            ws.onmessage = event => {
                if (ended || generation !== this.generation)
                    return;
                keepalive();
                try {
                    const frame = JSON.parse(event.data) as Frame;
                    validateFrame(frame);
                    const work = this.receiver.receive(frame);
                    if (frame.t === "ready") {
                        this.options.onState?.("ready");
                        this.cancel(this.stable);
                        // Rule 5: `k` returns to 0 once the connection has
                        // stayed ready for 10 s after its history caught up,
                        // never while it is still behind.
                        void work.then(() => {
                            if (!ended && generation === this.generation && !this.stopped) {
                                this.cancel(this.stable);
                                this.stable = this.later(() => { this.attempt = 0; }, 10000);
                            }
                        }, () => { });
                    }
                    void work.catch(e => {
                        if (ended || generation !== this.generation || this.stopped)
                            return;
                        this.options.onError?.(e);
                        disconnected(1006);
                        ws.close();
                    });
                }
                catch (e) {
                    this.options.onError?.(e);
                    disconnected(1006);
                    ws.close();
                }
            };
            ws.onclose = e => disconnected(e.code);
            ws.onerror = () => { disconnected(1006); ws.close(); };
        }
        catch (e) {
            this.options.onError?.(e);
            disconnected(1006);
        }
    }
    /** The `limit` the next history request asks for: halved after each
     * 5xx but 503 (a page the front could not serve in its budget), doubled back
     * after each answer, up to the front's own page. */
    private limit = HISTORY_PAGE;
    private history(p?: Position, recovery?: AbortSignal): Promise<History> {
        const signal = AbortSignal.any([this.abort.signal, ...(recovery ? [recovery] : [])]);
        return this.fetchHistory(p, signal);
    }
    /** Rule 6: a 429 or any 5xx is retried, the socket kept, after
     * `Retry-After` plus full jitter; only a 4xx is an error. Each request
     * has a 30 s deadline; the waits between them do not count. */
    private async fetchHistory(p: Position | undefined, signal: AbortSignal): Promise<History> {
        for (let k = 0;; k++) {
            signal.throwIfAborted();
            const url = new URL(this.options.history);
            if (p)
                url.searchParams.set("after", `${p.e}.${p.n}`);
            if (this.limit < HISTORY_PAGE)
                url.searchParams.set("limit", String(this.limit));
            const deadline = new AbortController();
            const timer = this.later(() => deadline.abort(Error("history deadline exceeded")), 30000);
            let response: Response, h: History | undefined;
            try {
                const attempt = AbortSignal.any([signal, deadline.signal]);
                response = await (this.options.fetch ?? fetch)(url.toString(), { signal: attempt });
                attempt.throwIfAborted();
                if (response.ok)
                    h = await response.json() as History;
                attempt.throwIfAborted();
            }
            finally {
                this.cancel(timer);
            }
            if (h) {
                this.limit = Math.min(HISTORY_PAGE, this.limit * 2);
                return h;
            }
            if (response.status !== 429 && response.status < 500)
                throw Error(`history answered ${response.status}`);
            // A 503 is the room unavailable (a takeover), not a page too
            // big for the front: it keeps the page.
            if (response.status >= 500 && response.status !== 503)
                this.limit = Math.max(1, Math.floor(this.limit / 2));
            const retry = response.headers.get("Retry-After") ?? "0";
            const seconds = Number(retry), delay = Number.isFinite(seconds) ? seconds * 1000 : Math.max(0, Date.parse(retry) - this.clock.now());
            await new Promise<void>((resolve, reject) => {
                const aborted = () => { this.cancel(id); reject(Error("closed")); };
                const id = this.later(() => { signal.removeEventListener("abort", aborted); resolve(); }, Math.max(0, delay || 0) + backoff(k, this.random));
                signal.addEventListener("abort", aborted, { once: true });
                if (signal.aborted)
                    aborted();
            });
        }
    }
}
