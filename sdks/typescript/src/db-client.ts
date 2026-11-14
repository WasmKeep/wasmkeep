// The database pattern's client (ADR 0029): reaches a database actor over
// `actors.call`, as the Rust SDK's `db::Client`. No storage import, so the
// unit tests run it under node against fake actors.

import { call } from "./call";
import {
  DbError,
  type Outcome,
  type Param,
  READ,
  type Rows,
  type Statement,
  WRITE,
  decodeReply,
  encodeRequest,
} from "./db-wire";

/** Reaches one database actor. Cheap to create. */
export class Client {
  /** The database at `address` (`class/name`, such as `db/main`). If the
   * name comes from a request (one database per tenant), check it against
   * the names you serve first. */
  constructor(readonly address: string) {}

  /** Runs one read; returns its rows. A read never changes the database:
   * it runs in a transaction that is rolled back. */
  query(sql: string, params: Param[] = []): Rows {
    const [r] = this.read([{ sql, params, rows: true }]);
    if (!("columns" in r)) throw new DbError("refused", "malformed reply from database", { status: 200 });
    return r;
  }

  /** Runs one write, durably; returns the number of rows it changed. */
  exec(sql: string, params: Param[] = []): number {
    const [r] = this.batch([{ sql, params }]);
    if (!("changes" in r)) throw new DbError("refused", "malformed reply from database", { status: 200 });
    return r.changes;
  }

  /** Runs `statements` in order in one transaction, committed durably once
   * all succeed: all are kept, or none. One outcome per statement. */
  batch(statements: readonly Statement[]): Outcome[] {
    return this.send(WRITE, statements);
  }

  /** Runs reads in order in one transaction, so they see the same data; it
   * is rolled back, so nothing they do is kept. */
  read(statements: readonly Statement[]): Outcome[] {
    return this.send(READ, statements);
  }

  private send(path: string, statements: readonly Statement[]): Outcome[] {
    const body = new TextEncoder().encode(encodeRequest(statements));
    const r = call(this.address, "POST", path, [["content-type", "application/json"]], body);
    if ("error" in r) throw new DbError("call", r.error);
    return decodeReply(r.reply.status, r.reply.body, statements);
  }
}

/** Rows as objects keyed by column name (a repeated column name keeps its
 * last value: read `rows` for those). */
export function objects(result: Rows): Record<string, unknown>[] {
  return result.rows.map((row) => {
    const o: Record<string, unknown> = {};
    result.columns.forEach((c, i) => (o[c] = row[i]));
    return o;
  });
}
