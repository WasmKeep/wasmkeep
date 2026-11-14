// Class `counter`: each actor (`counter/alice`, `counter/bob`, ...) is a
// counter of its own, in its own SQLite database, so it survives restarts.
//
// - `GET /` answers the value
// - `POST /` adds one and answers the new value
import { defineActor, storage } from "@wasmkeep/sdk";

export default defineActor({
  // Runs before the actor's first request, every time it starts.
  init() {
    storage.exec("CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)");
    storage.exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)");
  },

  fetch(req) {
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
