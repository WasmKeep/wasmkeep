import { DurableObject } from "cloudflare:workers";

// One object per name; the value lives in the object's SQLite database.
export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    ctx.storage.sql.exec(
      "CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)");
    ctx.storage.sql.exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)");
  }
  async fetch(request) {
    const sql = this.ctx.storage.sql;
    if (request.method === "GET") {
      return new Response(`${sql.exec("SELECT value FROM counter WHERE id = 1").one().value}\n`);
    }
    if (request.method === "POST") {
      return new Response(`${sql.exec("UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value").one().value}\n`);
    }
    return new Response("use GET or POST\n", { status: 405 });
  }
}

// /counter/<name> -> the object named <name>.
export default {
  async fetch(request, env) {
    const name = new URL(request.url).pathname.split("/")[2] ?? "default";
    return env.COUNTER.get(env.COUNTER.idFromName(name)).fetch(request);
  },
};
