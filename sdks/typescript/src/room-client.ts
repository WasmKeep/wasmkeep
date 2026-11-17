/** Browser-only room transport: no WASI, actor, or Node imports. */
import type { Position, Item } from "./room-edge";
export type { Position } from "./room-edge";
export type Frame = { t: "m"; e: number; b: number; n: number; m: Item[] } | { t: "ready"; e: number; n: number | null } | { t: "behind" | "pong"; e: number; n: number };
export interface History {
  head: Position; lost?: { e: number; base: number }; reset?: Position;
  messages: { position: Position; at: number; id?: string; body: string }[]; more: boolean;
}
export interface ReceiverOptions {
  history(after?: Position): Promise<History>;
  onMessage?(p: Position, body: string, id?: string, at?: number): void;
  onLost?(first: number, last: number): void;
  onReset?(p: Position): void;
}
export function backoff(k: number, random = Math.random, closeCode?: number): number {
  return random() * (k === 0 && (closeCode === 4801 || closeCode === 1012) ? 2000 : Math.min(30000, 500 * 2 ** Math.min(k, 16)));
}
export function joinURL(url: string, last?: Position): string {
  const u = new URL(url);
  if (last) { u.searchParams.set("e", String(last.e)); u.searchParams.set("n", String(last.n)); }
  return u.toString();
}
/** Serializes live frames behind gap recovery, including multi-page history. */
export class Receiver {
  last?: Position;
  private work: Promise<void> = Promise.resolve();
  constructor(private options: ReceiverOptions, last?: Position) { this.last = last; }
  receive(frame: Frame): Promise<void> {
    const next = this.work.then(() => this.apply(frame));
    this.work = next.catch(() => {});
    return next;
  }
  private message(p: Position, body: string, id?: string, at?: number) {
    if (this.last && (p.e < this.last.e || (p.e === this.last.e && p.n <= this.last.n))) return;
    this.options.onMessage?.(p, body, id, at); this.last = p;
  }
  private async recover() {
    for (;;) {
      const h = await this.options.history(this.last);
      if (h.lost) {
        if (this.last && h.lost.base < this.last.n) this.options.onLost?.(h.lost.base + 1, this.last.n);
        this.last = { e: h.lost.e, n: h.lost.base };
      }
      if (h.reset) { this.options.onReset?.(h.reset); this.last = { e: h.reset.e, n: h.reset.n - 1 }; }
      for (const m of h.messages) this.message(m.position, m.body, m.id, m.at);
      if (!h.more) {
        // A clean epoch change with no new messages still advances the epoch.
        if (!this.last || h.head.n === this.last.n) this.last = h.head;
        return;
      }
    }
  }
  private async apply(f: Frame) {
    if (f.t === "ready" && !this.last && f.n !== null) { this.last = { e: f.e, n: f.n }; return; }
    if (f.t !== "m") {
      if (f.t === "behind" || f.n === null || !this.last || f.e !== this.last.e || f.n !== this.last.n) await this.recover();
      return;
    }
    if (!this.last || f.e > this.last.e + 1) await this.recover();
    if (this.last && f.e === this.last.e + 1) {
      if (f.b < this.last.n) { this.options.onLost?.(f.b + 1, this.last.n); this.last = { e: f.e, n: f.b }; }
      else this.last = { e: f.e, n: this.last.n };
    }
    if (this.last && f.e < this.last.e) return;
    if (!this.last || f.n > this.last.n + 1) await this.recover();
    for (let i = 0; i < f.m.length; i++) {
      const m = f.m[i], p = { e: f.e, n: f.n + i };
      if (this.last && p.e === this.last.e && p.n > this.last.n + 1) throw Error("history did not fill gap");
      this.message(p, m.d, m.i, m.a);
    }
  }
}
export interface Clock { now(): number; set(fn: () => void, ms: number): unknown; clear(id: unknown): void }
export interface Socket {
  onopen: (() => void) | null; onmessage: ((event: { data: string }) => void) | null;
  onclose: ((event: { code: number }) => void) | null; onerror: (() => void) | null;
  send(s: string): void; close(): void;
}
export interface ClientOptions extends Omit<ReceiverOptions, "history"> {
  join: string; history: string; last?: Position;
  random?: () => number; clock?: Clock;
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
    this.receiver = new Receiver({ ...options, history: p => this.history(p) }, options.last);
  }
  get last() { return this.receiver.last; }
  private later(fn: () => void, ms: number) {
    const id = this.clock.set(() => { this.timers.delete(id); fn(); }, ms); this.timers.add(id); return id;
  }
  private cancel(id: unknown) { this.clock.clear(id); this.timers.delete(id); }
  connect() { if (!this.stopped) return; this.stopped = false; this.abort = new AbortController(); this.open(); }
  close() {
    this.stopped = true; this.generation++; this.abort.abort();
    for (const id of this.timers) this.clock.clear(id); this.timers.clear();
    this.socket?.close(); this.options.onState?.("closed");
  }
  private open() {
    if (this.stopped) return;
    const generation = ++this.generation;
    this.options.onState?.("connecting");
    let ended = false;
    const disconnected = (code: number) => {
      if (ended || generation !== this.generation || this.stopped) return;
      ended = true; this.cancel(this.heartbeat); this.cancel(this.stable);
      this.options.onState?.("disconnected");
      this.later(() => this.open(), backoff(this.attempt++, this.random, code));
    };
    try {
      const ws = this.socket = (this.options.socket ?? (url => new WebSocket(url) as unknown as Socket))(joinURL(this.options.join, this.last));
      const keepalive = () => {
        this.cancel(this.heartbeat);
        this.heartbeat = this.later(() => {
          try { ws.send('{"t":"ping"}'); } catch { disconnected(1006); ws.close(); return; }
          this.heartbeat = this.later(() => { disconnected(1006); ws.close(); }, 10000);
        }, 45000);
      };
      ws.onopen = keepalive;
      ws.onmessage = event => {
        if (ended || generation !== this.generation) return;
        keepalive();
        try {
          const frame = JSON.parse(event.data) as Frame;
          if (frame.t === "ready") {
            this.options.onState?.("ready");
            this.cancel(this.stable); this.stable = this.later(() => { this.attempt = 0; }, 10000);
          }
          void this.receiver.receive(frame).catch(e => { this.options.onError?.(e); disconnected(1006); ws.close(); });
        } catch (e) { this.options.onError?.(e); disconnected(1006); ws.close(); }
      };
      ws.onclose = e => disconnected(e.code);
      ws.onerror = () => { disconnected(1006); ws.close(); };
    } catch (e) { this.options.onError?.(e); disconnected(1006); }
  }
  private async history(p?: Position): Promise<History> {
    const url = new URL(this.options.history);
    if (p) url.searchParams.set("after", `${p.e}.${p.n}`);
    for (let k = 0; ; k++) {
      const response = await (this.options.fetch ?? fetch)(url.toString(), { signal: this.abort.signal });
      if (response.status !== 429 && response.status !== 503) {
        if (!response.ok) throw Error(`history answered ${response.status}`);
        return await response.json() as History;
      }
      const retry = response.headers.get("Retry-After") ?? "0";
      const seconds = Number(retry), delay = Number.isFinite(seconds) ? seconds * 1000 : Math.max(0, Date.parse(retry) - this.clock.now());
      await new Promise<void>((resolve, reject) => {
        const signal = this.abort.signal;
        const aborted = () => { this.cancel(id); reject(Error("closed")); };
        const id = this.later(() => { signal.removeEventListener("abort", aborted); resolve(); }, Math.max(0, delay || 0) + backoff(k, this.random));
        signal.addEventListener("abort", aborted, { once: true });
        if (signal.aborted) aborted();
      });
    }
  }
}
