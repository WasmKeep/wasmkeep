// A counter actor in TypeScript. Each actor name is its own counter:
// GET returns the current value, POST increments it and returns the new one.
// The value lives in the actor's SQLite database, so it survives restarts.
import { defineActor, storage } from "@wasmkeep/sdk";
import { work } from "./work";

export default defineActor({
  init() {
    storage.exec(
      "CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)",
    );
    storage.exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)");
  },

  fetch(req) {
    // A benchmark route (see work.ts), not part of the counter.
    if (req.path === "/work") {
      return `${work()}\n`;
    }
    if (req.method === "GET") {
      const [row] = storage.query("SELECT value FROM counter WHERE id = 1");
      return `${row.value}\n`;
    }
    if (req.method === "POST") {
      const [row] = storage.query("UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value");
      return `${row.value}\n`;
    }
    return { status: 405, body: "use GET or POST\n" };
  },
});
