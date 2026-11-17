/** ADR 0040 R2. One Core per component instance, never per request. */
import type { Value, Rows } from "wasmkeep:actor/types@0.2.0";
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
const enc = new TextEncoder();
const size = (s: string) => enc.encode(s).length;
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
export class Core {
    private epoch?: Epoch;
    private pending = false;
    constructor(private db: Store, private alarms: {
        setAlarm(n: bigint): void;
    }, private confirm = false) {
    }
    init() {
        this.db.exec("CREATE TABLE IF NOT EXISTS wasmkeep_room_history (n INTEGER PRIMARY KEY,e INTEGER NOT NULL,at INTEGER NOT NULL,id TEXT UNIQUE,body TEXT NOT NULL)", []);
        this.db.exec("CREATE TABLE IF NOT EXISTS wasmkeep_room_epochs (e INTEGER PRIMARY KEY,base INTEGER NOT NULL)", []);
    }
    private messages(): Message[] {
        return this.db.query("SELECT n,e,at,id,body FROM wasmkeep_room_history ORDER BY n", []).values.map(r => ({ position: { n: num(r[0]), e: num(r[1]) }, at: num(r[2]), id: r[3].tag === "null" ? undefined : str(r[3]), body: str(r[4]) }));
    }
    private epochs(): Epoch[] {
        return this.db.query("SELECT e,base FROM wasmkeep_room_epochs ORDER BY e", []).values.map(r => ({ e: num(r[0]), base: num(r[1]) }));
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
    private wake(): Epoch {
        if (this.epoch)
            return this.epoch;
        const e = (this.epochs().at(-1)?.e ?? 0n) + 1n, base = this.messages().at(-1)?.position.n ?? 0n;
        this.transaction(true, () => {
            this.db.exec("INSERT INTO wasmkeep_room_epochs(e,base) VALUES(?1,?2)", [integer(e), integer(base)]);
            this.db.exec("DELETE FROM wasmkeep_room_epochs WHERE e <= ?1", [integer(e > 64n ? e - 64n : 0n)]);
        });
        return this.epoch = { e, base };
    }
    publish(input: Publish[], now: bigint): Position[] {
        if (!Array.isArray(input) || input.length === 0 || input.length > 64 || input.some(m => typeof m.body !== "string" || size(m.body) > 65536 || (m.id !== undefined && (typeof m.id !== "string" || size(m.id) > 64))) || input.reduce((n, m) => n + size(m.body), 0) > 262144)
            throw Error("invalid publish");
        const at = integer(now), epoch = this.wake(), rows = this.messages();
        let n = rows.at(-1)?.position.n ?? epoch.base;
        const positions = this.transaction(false, () => {
            const positions: Position[] = [];
            for (const m of input) {
                const old = rows.find(r => m.id !== undefined && r.id === m.id);
                if (old) {
                    positions.push(old.position);
                    continue;
                }
                const p = { e: epoch.e, n: ++n };
                this.db.exec("INSERT INTO wasmkeep_room_history(n,e,at,id,body) VALUES(?1,?2,?3,?4,?5)", [integer(n), integer(epoch.e), at, m.id === undefined ? { tag: "null" } : text(m.id), text(m.body)]);
                rows.push({ position: p, at: now, ...m });
                positions.push(p);
            }
            let bytes = rows.reduce((n, r) => n + size(r.body), 0), drop = 0;
            while (rows.length - drop > 2000 || bytes > 8 * 1024 * 1024) {
                bytes -= size(rows[drop].body);
                drop++;
            }
            if (drop)
                this.db.exec("DELETE FROM wasmkeep_room_history WHERE n <= ?1", [integer(rows[drop - 1].position.n)]);
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
        if (!Number.isInteger(limit) || limit < 1 || limit > 2000 || (last !== undefined && (!Number.isInteger(last) || last < 0 || last > 2000)) || (after && last !== undefined))
            throw Error("invalid history request");
        const epoch = this.wake(), rows = this.messages(), epochs = this.epochs();
        const head = { e: epoch.e, n: rows.at(-1)?.position.n ?? epoch.base };
        const h: History = { head, messages: [], more: false };
        let start = rows.length;
        if (after) {
            if (after.e <= 0n || after.n < 0n || after.e > epoch.e || (after.e === epoch.e && after.n > head.n))
                throw Error("invalid position");
            let n = after.n;
            const boundary = epochs.filter(e => e.e > after.e).reduce<Epoch | undefined>((a, b) => !a || b.base < a.base ? b : a, undefined);
            if (after.e >= epochs[0].e && boundary && after.n > boundary.base) {
                h.lost = boundary;
                n = boundary.base;
            }
            if (after.e < epochs[0].e || (rows[0] && n + 1n < rows[0].position.n)) {
                h.reset = rows[0]?.position ?? head;
                start = 0;
            }
            else {
                start = rows.findIndex(m => m.position.n > n);
                if (start < 0)
                    start = rows.length;
            }
        }
        else if (last !== undefined)
            start = Math.max(0, rows.length - last);
        let bytes = 48 + (h.lost ? 16 : 0) + (h.reset ? 16 : 0);
        for (const row of rows.slice(start)) {
            const encoded = 36 + size(row.body) + (row.id === undefined ? 0 : 4 + size(row.id));
            if (h.messages.length === limit || bytes + encoded > 262144) {
                h.more = true;
                break;
            }
            bytes += encoded;
            h.messages.push(row);
        }
        return h;
    }
}
/** WIT storage throws a record, while application stores may throw Error. */
export function errorMessage(e: unknown): string {
    return typeof e === "object" && e !== null && "message" in e ? String(e.message) : String(e);
}
