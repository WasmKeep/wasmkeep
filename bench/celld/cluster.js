import { DurableObject } from "cloudflare:workers";
import counter, { Counter } from "./counter.js";

// The three-node comparison's worker: the workerd counter unchanged, and a
// ledger used by the takeover check alone (the counterpart of bench/ledger).
export { Counter };

// One object per name; each POST ?t=<integer> records its token, GET returns
// them all, one per line, in insertion order.
export class Ledger extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec("CREATE TABLE IF NOT EXISTS ledger (token INTEGER NOT NULL)");
  }
  async fetch(request) {
    const sql = this.ctx.storage.sql;
    if (request.method === "GET") {
      const tokens = sql.exec("SELECT token FROM ledger ORDER BY rowid").toArray();
      return new Response(tokens.map((row) => `${row.token}\n`).join(""));
    }
    if (request.method === "POST") {
      const token = Number(new URL(request.url).searchParams.get("t"));
      if (!Number.isInteger(token)) {
        return new Response("POST ?t=<integer>\n", { status: 400 });
      }
      sql.exec("INSERT INTO ledger (token) VALUES (?)", token);
      return new Response("ok\n");
    }
    return new Response("use GET or POST\n", { status: 405 });
  }
}

// /ledger/<name> -> the ledger named <name>; anything else is the counter's.
export default {
  async fetch(request, env) {
    if (request.url.includes("/ledger/")) {
      const name = new URL(request.url).pathname.split("/")[2] ?? "default";
      return env.LEDGER.get(env.LEDGER.idFromName(name)).fetch(request);
    }
    return counter.fetch(request, env);
  },
};
