/++
A counter actor in D (LDC, -betterC), with the WasmKeep D SDK (sdks/d).
GET returns the value, POST increments it; each actor name is its own
counter, kept in its SQLite database.
+/
module counter;

import wasmkeep;

struct Counter {
    static void init() {
        if (exec("CREATE TABLE IF NOT EXISTS counter"
                ~ " (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)") < 0
            || exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)") < 0)
            fail(eventCopy(lastError()));
    }

    static Response fetch(ref Request req) {
        const(char)[] sql;
        if (req.method == "GET")
            sql = "SELECT value FROM counter WHERE id = 1";
        else if (req.method == "POST")
            sql = "UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value";
        else
            return replyText(405, "use GET or POST\n");
        auto rows = query(sql);
        if (!rows.ok || rows.count == 0)
            return reply(500, "%.*s\n", cast(int) lastError().length, lastError().ptr);
        return reply(200, "%lld\n", rows.getInt(0, 0));
    }
}

mixin Actor!Counter;
