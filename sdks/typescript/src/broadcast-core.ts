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
export class Core {
    private epoch?: Epoch;
    private pending = false;
    constructor(private db: Store, private alarms: {
        setAlarm(n: bigint): void;
    }, private confirm = false) {
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
    /** Up to `count` rows from number `from` on, at most one page past
     * `bytes` of bodies. */
    rows(from: bigint, count: number, bytes = Infinity): Row[] {
        const out: Row[] = [];
        let seen = 0;
        while (out.length < count && seen <= bytes) {
            const page = this.db.query(`SELECT n,e,at,id,body,length(CAST(body AS BLOB)),coalesce(length(CAST(id AS BLOB)),0) FROM ${H} WHERE n >= ?1 ORDER BY n LIMIT ?2`, [integer(from), integer(BigInt(Math.min(PAGE, count - out.length)))]).values;
            for (const r of page) {
                const row: Row = { position: { n: num(r[0]), e: num(r[1]) }, at: num(r[2]), id: r[3].tag === "null" ? undefined : str(r[3]), body: str(r[4]), bodyBytes: Number(num(r[5])), idBytes: Number(num(r[6])) };
                out.push(row);
                seen += row.bodyBytes;
                from = row.position.n + 1n;
            }
            if (page.length < PAGE)
                break;
        }
        return out;
    }
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
        if (!this.pending) {
            this.alarms.setAlarm(now + 20n);
            this.pending = true;
        }
        return positions;
    }
    beforePush(now: bigint): boolean {
        this.wake();
        if (this.confirm)
            try {
                this.db.flush();
            }
            catch (e) {
                if (errorMessage(e).includes("not durable: still uploading")) {
                    this.alarms.setAlarm(now + 50n);
                    this.pending = true;
                    return false;
                }
                throw e;
            }
        this.pending = false;
        return true;
    }
    history(after?: Position, limit = 2000, last?: number): History {
        return this.page(after, limit, last).history;
    }
    /** `history`, and with `raw` each message's bytes for the wire answer
     * (then each message's `body` is left empty). */
    page(after?: Position, limit = 2000, last?: number, raw = false): { history: History; raw: Raw[] } {
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
