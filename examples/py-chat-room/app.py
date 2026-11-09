"""A chat room in Python. Each actor name is one room: the Rust
examples/chat-room, line for line in behaviour.

- Connect a WebSocket to /actor/<class>/<room>: you receive the last 20
  messages, then everything said in the room. Text you send is broadcast to
  everyone in the room, you included. If the operator set MOTD (wasmkeep
  serve --env chat=FILE), it comes first, as "* <MOTD>".
- POST / with a text body says something over plain HTTP.
- GET / lists the recent messages.

Messages are kept in the room's SQLite database (the last 100). The host owns
the sockets, so the room can hibernate while people stay connected: the next
message wakes it.
"""
import time

from wasmkeep import Actor, Response, actors, env, sockets, storage

HISTORY = 20
KEEP = 100


def recent(limit):
    rows = storage.query(
        "SELECT text FROM (SELECT id, text FROM messages ORDER BY id DESC LIMIT ?) ORDER BY id",
        limit,
    )
    return [row["text"] for row in rows]


def say(text):
    storage.exec("INSERT INTO messages (at, text) VALUES (?, ?)", int(time.time() * 1000), text)
    storage.exec("DELETE FROM messages WHERE id <= (SELECT max(id) FROM messages) - ?", KEEP)
    # A slow reader drops this message; the others still get it.
    sockets.broadcast(text)


def announce(what):
    """"* someone joined #lobby (2 online)": the room is this actor's name."""
    room = actors.self_address().name
    sockets.broadcast(f"* someone {what} #{room} ({len(sockets.connections())} online)")


class ChatRoom(Actor):
    def init(self):
        storage.exec(
            "CREATE TABLE IF NOT EXISTS messages"
            " (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, text TEXT NOT NULL)"
        )

    def websocket_open(self, conn, path):
        motd = env.get("MOTD")
        if motd is not None:
            sockets.send(conn, f"* {motd}")
        for text in recent(HISTORY):
            sockets.send(conn, text)
        announce("joined")

    def websocket_message(self, conn, data):
        if isinstance(data, str):
            say(data)

    def websocket_close(self, conn, code, reason):
        announce("left")

    def fetch(self, req):
        if req.method == "POST":
            body = req.bytes()
            try:
                text = body.decode().strip() if len(body) <= 4096 else ""
            except UnicodeDecodeError:
                text = ""
            if not text:
                return Response("send a UTF-8 text body\n", 400)
            say(text)
            return "sent\n"
        if req.method == "GET":
            return "\n".join(recent(HISTORY)) + "\n"
        return Response("use GET or POST\n", 405)


WitWorld, IncomingHandler = ChatRoom.exports()
