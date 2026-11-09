"""A counter actor in Python: GET reads, POST increments, per actor name."""
from wasmkeep import Actor, Response, storage


class Counter(Actor):
    def init(self):
        storage.exec(
            "CREATE TABLE IF NOT EXISTS counter"
            " (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)"
        )
        storage.exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)")

    def fetch(self, req):
        if req.method == "GET":
            [row] = storage.query("SELECT value FROM counter WHERE id = 1")
        elif req.method == "POST":
            [row] = storage.query(
                "UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value"
            )
        else:
            return Response("use GET or POST\n", 405)
        return f"{row['value']}\n"


WitWorld, IncomingHandler = Counter.exports()
