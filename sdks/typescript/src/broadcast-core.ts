/** ADR 0040 R2. One Core per component instance, never per request. */
import type { Value, Rows } from "wasmkeep:actor/types@0.2.0";
import { byteLength, fits } from "./room-utf8";
export interface Position {
    e: bigint;
    n: bigint;
}
export interface Epoch {
    e: bigint;
    base: bigint;
}
export interface Publish {
    body: string;
    id?: string;
}
export interface Message {
    position: Position;
    at: bigint;
    id?: string;
    body: string;
}
export interface History {
    head: Position;
    lost?: Epoch;
    reset?: Position;
    messages: Message[];
    more: boolean;
}
export interface Store {
    exec(s: string, p: Value[]): bigint;
    execDurable(s: string, p: Value[]): bigint;
    query(s: string, p: Value[]): Rows;
    flush(): void;
}
const max = 0x7fffffffffffffffn;
const integer = (val: bigint): Value => {
    if (val < 0n || val > max)
        throw Error("integer overflow");
    return { tag: "integer", val };
};
const text = (val: string): Value => ({ tag: "text", val });
const num = (v: Value) => {
    if (v.tag !== "integer" || v.val < 0n)
        throw Error("invalid room integer");
    return v.val;
};
const str = (v: Value) => {
    if (v.tag !== "text")
        throw Error("invalid room text");
    return v.val;
};
const H = "wasmkeep_room_history";
/** Rows a bounded read takes at a time. */
const PAGE = 32;
/** A browser history page (`history-json`): at most this many rows and
 * this many bytes of bodies, and at least one row. */
export const JSON_ROWS = 256, JSON_BYTES = 64 * 1024;
/** The JSON numbers a browser reads exactly: at most 2^53 - 1. */
const SAFE = 9007199254740991n;
const safe = (v: bigint) => {
    if (v > SAFE)
        throw Error("invalid browser integer");
    return v.toString();
};
/** One browser page's messages, rendered by SQLite: from `?1`, at most
 * `?2 - 1` rows (`?2` rows are read to tell `more`), and past the first
 * row at most `?3` bytes of bodies. Answers the messages as JSON (without
 * the brackets), the rows kept, the rows read and the largest `at`.
 * `json_quote` escapes as `JSON.stringify` does (the vectors check it):
 * `"`, `\` and controls only, short forms for \b \t \n \f \r. Shared with
 * the Rust room (crates/wasmkeep-sdk/src/broadcast/core.rs). */
export const JSON_PAGE = `WITH page AS (SELECT n,e,at,id,body,row_number() OVER w AS k,sum(length(CAST(body AS BLOB))) OVER w AS total FROM (SELECT n,e,at,id,body FROM ${H} WHERE n >= ?1 ORDER BY n LIMIT ?2) WINDOW w AS (ORDER BY n ROWS UNBOUNDED PRECEDING)), kept AS (SELECT * FROM page WHERE k < ?2 AND (k = 1 OR total <= ?3)) SELECT CAST(group_concat('{"position":{"e":'||e||',"n":'||n||'},"at":'||at||CASE WHEN id IS NULL THEN '' ELSE ',"id":'||json_quote(id) END||',"body":'||json_quote(body)||'}', ',' ORDER BY n) AS BLOB),(SELECT count(*) FROM kept),(SELECT count(*) FROM page),max(at) FROM kept`;
/** A history row with its UTF-8 sizes, as SQLite counts them. */
export interface Row extends Message {
    bodyBytes: number;
    idBytes: number;
}
/** Raw bytes of a history page's rows, for the wire answer. */
export interface Raw {
    id?: Uint8Array;
    body: Uint8Array;
}
const bytesOf = (v: Value): Uint8Array => {
    if (v.tag === "blob")
        return v.val;
    if (v.tag === "text")
        return new TextEncoder().encode(v.val);
    throw Error("invalid room blob");
};
/** Every per-event path reads what it answers, never the whole window: the
 * head and an id are indexed lookups, trimming is one statement SQLite
 * runs, and history and pushes read bounded pages. Sizes come from SQLite
 * (`length(CAST(... AS BLOB))`), not from re-encoding text in the guest. */
/** What a pull carries (ADR 0040, section 13): the push header a child at
 * the pulled position needs, then the messages after it, each with its own
 * epoch and that epoch's base. */
export interface Pull {
    e: bigint;
    b: bigint;
    first: bigint;
    head: bigint;
    recover: boolean;
    messages: Pulled[];
}
export interface Pulled extends Row {
    /** The base of the message's own epoch. */
    b: bigint;
}
/** Rows one pull carries at most, and the bodies they total at most (the
 * old round's slice: one push takes 512 messages and 512 KiB). */
const PULL_ROWS = 513, PULL_BYTES = 512 * 1024;
/** What reading one row costs a pull's budget besides its bytes. */
export const ROW_COST = 1024;
/** A busy flag older than this without a pull counts as idle: longer than
 * an event's wall (30 s by default), so a round held by a slow edge keeps
 * its pusher busy, and a pusher that lost its alarm is woken again. */
export const IDLE_EXPIRY = 60000;
export class Core {
    private epoch?: Epoch;
    /** The pusher said it went idle at the head (section 13); a cold room
     * assumes so. Memory only. */
    private idle = true;
    private pulled?: number;
    constructor(private db: Store, private confirm = false) {
    }
    init() {
        this.db.exec(`CREATE TABLE IF NOT EXISTS ${H} (n INTEGER PRIMARY KEY,e INTEGER NOT NULL,at INTEGER NOT NULL,id TEXT UNIQUE,body TEXT NOT NULL)`, []);
        this.db.exec("CREATE TABLE IF NOT EXISTS wasmkeep_room_epochs (e INTEGER PRIMARY KEY,base INTEGER NOT NULL)", []);
    }
    /** The whole window, oldest first. Not used on any per-event path. */
    messages(): Message[] {
        return this.db.query(`SELECT n,e,at,id,body FROM ${H} ORDER BY n`, []).values.map(r => ({ position: { n: num(r[0]), e: num(r[1]) }, at: num(r[2]), id: r[3].tag === "null" ? undefined : str(r[3]), body: str(r[4]) }));
    }
    epochs(): Epoch[] {
        return this.db.query("SELECT e,base FROM wasmkeep_room_epochs ORDER BY e", []).values.map(r => ({ e: num(r[0]), base: num(r[1]) }));
    }
    private edge(newest: boolean): Position | undefined {
        const r = this.db.query(`SELECT n,e FROM ${H} ORDER BY n ${newest ? "DESC" : "ASC"} LIMIT 1`, []).values[0];
        return r && { n: num(r[0]), e: num(r[1]) };
    }
    /** The highest number in the window, if any. */
    last(): bigint | undefined { return this.edge(true)?.n; }
    private transaction<T>(durable: boolean, f: () => T): T {
        this.db.exec("BEGIN", []);
        try {
            const result = f();
            if (durable)
                this.db.execDurable("COMMIT", []);
            else
                this.db.exec("COMMIT", []);
            return result;
        }
        catch (e) {
            try {
                this.db.exec("ROLLBACK", []);
            }
            catch {
            }
            throw e;
        }
    }
    wake(): Epoch {
        if (this.epoch)
            return this.epoch;
        const e = (this.epochs().at(-1)?.e ?? 0n) + 1n, base = this.last() ?? 0n;
        this.transaction(true, () => {
            this.db.exec("INSERT INTO wasmkeep_room_epochs(e,base) VALUES(?1,?2)", [integer(e), integer(base)]);
            this.db.exec("DELETE FROM wasmkeep_room_epochs WHERE e <= ?1", [integer(e > 64n ? e - 64n : 0n)]);
        });
        return this.epoch = { e, base };
    }
    publish(input: Publish[], now: bigint): Position[] {
        if (!Array.isArray(input) || input.length === 0 || input.length > 64 || input.some(m => typeof m.body !== "string" || !fits(m.body, 65536) || (m.id !== undefined && (typeof m.id !== "string" || !fits(m.id, 64)))) || !within(input, 262144))
            throw Error("invalid publish");
        const at = integer(now), epoch = this.wake();
        let n = this.last() ?? epoch.base;
        const positions = this.transaction(false, () => {
            const positions: Position[] = [];
            for (const m of input) {
                const old = m.id === undefined ? undefined : this.db.query(`SELECT n,e FROM ${H} WHERE id=?1`, [text(m.id)]).values[0];
                if (old) {
                    positions.push({ n: num(old[0]), e: num(old[1]) });
                    continue;
                }
                const p = { e: epoch.e, n: ++n };
                this.db.exec(`INSERT INTO ${H}(n,e,at,id,body) VALUES(?1,?2,?3,?4,?5)`, [integer(n), integer(epoch.e), at, m.id === undefined ? { tag: "null" } : text(m.id), text(m.body)]);
                positions.push(p);
            }
            // The window: the newest 2,000 rows within 8 MiB of bodies; every older row goes.
            this.db.exec(`DELETE FROM ${H} WHERE n <= (SELECT n FROM (SELECT n, row_number() OVER w AS k, sum(length(CAST(body AS BLOB))) OVER w AS tail FROM ${H} WINDOW w AS (ORDER BY n DESC ROWS UNBOUNDED PRECEDING)) WHERE k > 2000 OR tail > 8388608 ORDER BY n DESC LIMIT 1)`, []);
            return positions;
        });
        return positions;
    }
    /** Whether a publish should ask its front to poke the pusher. */
    poke(now: number): boolean {
        return this.idle || this.pulled === undefined || now - this.pulled > IDLE_EXPIRY;
    }
    /** The pusher's read (section 13): the header a child at `after` needs
     * and the messages after it, at most `budget` bytes charged one
     * `ROW_COST` per row (at least one row unless `budget` is 0), 513 rows
     * and 512 KiB of bodies and ids. With `confirm`, it flushes first and
     * throws `not durable: still uploading` until the upload is in. `idle`
     * with `budget` 0 and no position, or the head, records that the pusher
     * stopped clocking itself; any other pull clears it. `raw` gives each
     * message's bytes, its text left empty. */
    pull(after: Position | undefined, budget: number, idle: boolean, now: number, raw = false): { pull: Pull; raw: Raw[] } {
        if (!Number.isSafeInteger(budget) || budget < 0 || budget > 1024 * 1024)
            throw Error("invalid pull budget");
        const epoch = this.wake();
        if (this.confirm)
            this.db.flush();
        const epochs = this.epochs(), head = this.last() ?? epoch.base;
        if (after && (after.e <= 0n || after.n < 0n || after.e > epoch.e || (after.e === epoch.e && after.n > head)))
            throw Error("invalid position");
        this.pulled = now;
        this.idle = idle && budget === 0 && (!after || (after.e === epoch.e && after.n === head));
        const later = after ? epochs.filter(e => e.e > after.e) : [];
        const b = later.reduce((b, e) => b < e.base ? b : e.base, epoch.base);
        const p: Pull = { e: epoch.e, b, first: head + 1n, head, recover: false, messages: [] };
        const out: Raw[] = [];
        // A position above a later epoch's base starts at that base: the numbers above it are void.
        const from = after ? later.reduce((n, e) => n < e.base ? n : e.base, after.n) : head;
        if (budget === 0 || from >= head)
            return { pull: p, raw: out };
        const rows = Math.max(1, Math.min(PULL_ROWS, Math.floor(budget / ROW_COST)));
        let next = from + 1n, bytes = 0;
        while (p.messages.length < rows) {
            const want = Math.min(PAGE, rows - p.messages.length);
            const page = this.db.query(`SELECT n,e,at,id,${raw ? "CAST(body AS BLOB),CAST(id AS BLOB)" : "body,NULL"},length(CAST(body AS BLOB)),coalesce(length(CAST(id AS BLOB)),0) FROM ${H} WHERE n >= ?1 ORDER BY n LIMIT ?2`, [integer(next), integer(BigInt(want))]).values;
            for (const r of page) {
                const n = num(r[0]), e = num(r[1]), bodyBytes = Number(num(r[6])), idBytes = Number(num(r[7]));
                if (p.messages.length && bytes + bodyBytes + idBytes > PULL_BYTES)
                    return { pull: p, raw: out };
                const boundary = epochs.find(x => x.e === e);
                if (!boundary) {
                    // Its epoch's base left the 64 kept: HTTP history answers it.
                    return { pull: { ...p, recover: true, first: head + 1n, messages: [] }, raw: [] };
                }
                if (!p.messages.length)
                    p.first = n;
                bytes += bodyBytes + idBytes;
                p.messages.push({ position: { n, e }, b: boundary.base, at: num(r[2]), id: r[3].tag === "null" ? undefined : str(r[3]), body: raw ? "" : str(r[4]), bodyBytes, idBytes });
                if (raw)
                    out.push({ body: bytesOf(r[4]), id: r[3].tag === "null" ? undefined : bytesOf(r[5]) });
                next = n + 1n;
            }
            if (page.length < want)
                break;
        }
        return { pull: p, raw: out };
    }
    history(after?: Position, limit = 2000, last?: number): History {
        return this.page(after, limit, last).history;
    }
    /** `history`, and with `raw` each message's bytes for the wire answer
     * (then each message's `body` is left empty). */
    page(after?: Position, limit = 2000, last?: number, raw = false): { history: History; raw: Raw[] } {
        const { history: h, from: start } = this.start(after, limit, last);
        let from = start;
        const out: Raw[] = [];
        let bytes = 48 + (h.lost ? 16 : 0) + (h.reset ? 16 : 0);
        while (from !== undefined && !h.more) {
            const page = this.db.query(`SELECT n,e,at,id,${raw ? "CAST(body AS BLOB),CAST(id AS BLOB)" : "body,NULL"},length(CAST(body AS BLOB)),coalesce(length(CAST(id AS BLOB)),0) FROM ${H} WHERE n >= ?1 ORDER BY n LIMIT ?2`, [integer(from), integer(BigInt(PAGE))]).values;
            for (const r of page) {
                const encoded = 36 + Number(num(r[6])) + (r[3].tag === "null" ? 0 : 4 + Number(num(r[7])));
                if (h.messages.length === limit || bytes + encoded > 262144) {
                    h.more = true;
                    break;
                }
                bytes += encoded;
                h.messages.push({ position: { n: num(r[0]), e: num(r[1]) }, at: num(r[2]), id: r[3].tag === "null" ? undefined : str(r[3]), body: raw ? "" : str(r[4]) });
                if (raw)
                    out.push({ body: bytesOf(r[4]), id: r[3].tag === "null" ? undefined : bytesOf(r[5]) });
                from = num(r[0]) + 1n;
            }
            if (page.length < PAGE)
                break;
        }
        return { history: h, raw: out };
    }
    /** The page `history(after, limit)` answers, as the browser's JSON
     * (`JSON.stringify(historyJSON(...))`, byte for byte), rendered by
     * SQLite: no body or id crosses into the guest, so a front that
     * forwards it pays nothing per message. At most `JSON_ROWS` rows and
     * `JSON_BYTES` of bodies, at least one row, `more` when cut. */
    pageJSON(after?: Position, limit = JSON_ROWS): Uint8Array {
        const { history: h, from } = this.start(after, limit);
        let rows: Uint8Array = new Uint8Array(), more = false;
        if (from !== undefined) {
            const r = this.db.query(JSON_PAGE, [integer(from), integer(BigInt(Math.min(limit, JSON_ROWS) + 1)), integer(BigInt(JSON_BYTES))]).values[0];
            more = num(r[1]) < num(r[2]);
            // Every other number on the page is at most the head's.
            if (r[3].tag !== "null")
                safe(num(r[3]));
            if (r[0].tag !== "null")
                rows = bytesOf(r[0]);
        }
        const p = (v: Position) => `{"e":${safe(v.e)},"n":${safe(v.n)}}`;
        const start = new TextEncoder().encode(`{"head":${p(h.head)}${h.lost ? `,"lost":{"e":${safe(h.lost.e)},"base":${safe(h.lost.base)}}` : ""}${h.reset ? `,"reset":${p(h.reset)}` : ""},"more":${more},"messages":[`);
        const out = new Uint8Array(start.length + rows.length + 2);
        out.set(start);
        out.set(rows, start.length);
        out.set([0x5d, 0x7d], start.length + rows.length); // "]}"
        return out;
    }
    /** What a history page starts with: its head, `lost` and `reset`, and
     * the number its messages start from (none: the head only). */
    private start(after?: Position, limit = 2000, last?: number): { history: History; from?: bigint } {
        if (!Number.isInteger(limit) || limit < 1 || limit > 2000 || (last !== undefined && (!Number.isInteger(last) || last < 0 || last > 2000)) || (after && last !== undefined))
            throw Error("invalid history request");
        const epoch = this.wake(), epochs = this.epochs(), first = this.edge(false);
        const head = { e: epoch.e, n: this.last() ?? epoch.base };
        const h: History = { head, messages: [], more: false };
        let from: bigint | undefined;
        if (after) {
            if (after.e <= 0n || after.n < 0n || after.e > epoch.e || (after.e === epoch.e && after.n > head.n))
                throw Error("invalid position");
            let n = after.n;
            const boundary = epochs.filter(e => e.e > after.e).reduce<Epoch | undefined>((a, b) => !a || b.base < a.base ? b : a, undefined);
            if (after.e >= epochs[0].e && boundary && after.n > boundary.base) {
                h.lost = boundary;
                n = boundary.base;
            }
            const kept = () => this.db.query(`SELECT 1 FROM ${H} WHERE n=?1 AND e=?2`, [integer(after.n), integer(after.e)]).values.length > 0;
            if ((after.e < epochs[0].e && !kept()) || (first && n + 1n < first.n)) {
                h.reset = first ?? head;
                from = first?.n;
            }
            else
                from = n + 1n;
        }
        else if (last !== undefined && last > 0) {
            const r = this.db.query(`SELECT n FROM ${H} ORDER BY n DESC LIMIT 1 OFFSET ?1`, [integer(BigInt(last - 1))]).values[0];
            from = r ? num(r[0]) : first?.n;
        }
        return { history: h, from };
    }
}
/** Whether the bodies are at most `max` bytes together. */
function within(input: Publish[], max: number): boolean {
    return input.reduce((n, m) => n + m.body.length, 0) * 3 <= max || input.reduce((n, m) => n + byteLength(m.body), 0) <= max;
}
/** WIT storage throws a record, while application stores may throw Error. */
export function errorMessage(e: unknown): string {
    // jco throws a WIT error as an Error whose message is "[object Object] (see error.payload)".
    const p = typeof e === "object" && e !== null ? (e as { payload?: unknown }).payload : undefined;
    if (typeof p === "object" && p !== null && "message" in p)
        return String((p as { message: unknown }).message);
    return typeof e === "object" && e !== null && "message" in e ? String(e.message) : String(e);
}
