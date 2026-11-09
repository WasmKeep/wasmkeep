// A chat room in TypeScript. Each actor name is one room: the Rust
// examples/chat-room, line for line in behaviour.
//
// - Connect a WebSocket to /actor/<class>/<room>: you receive the last 20
//   messages, then everything said in the room. Text you send is broadcast
//   to everyone in the room, you included. If the operator set MOTD
//   (wasmkeep serve --env chat=FILE), it comes first, as "* <MOTD>".
// - POST / with a text body says something over plain HTTP.
// - GET / lists the recent messages.
//
// Messages are kept in the room's SQLite database (the last 100). The host
// owns the sockets, so the room can hibernate while people stay connected:
// the next message wakes it.
import { actors, defineActor, env, sockets, storage } from "@wasmkeep/sdk";

const HISTORY = 20;
const KEEP = 100;

function recent(limit: number): string[] {
  return storage
    .query("SELECT text FROM (SELECT id, text FROM messages ORDER BY id DESC LIMIT ?) ORDER BY id", limit)
    .map((row) => String(row.text));
}

function say(text: string): void {
  storage.exec("INSERT INTO messages (at, text) VALUES (?, ?)", Date.now(), text);
  storage.exec("DELETE FROM messages WHERE id <= (SELECT max(id) FROM messages) - ?", KEEP);
  // A slow reader drops this message; the others still get it.
  sockets.broadcast(text);
}

// "* someone joined #lobby (2 online)": the room is this actor's name.
const announce = (what: string) =>
  sockets.broadcast(`* someone ${what} #${actors.self().name} (${sockets.connections().length} online)`);

export default defineActor({
  init() {
    storage.exec(
      "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, text TEXT NOT NULL)",
    );
  },

  websocket: {
    open(conn) {
      const motd = env.get("MOTD");
      if (motd !== undefined) sockets.send(conn, `* ${motd}`);
      for (const text of recent(HISTORY)) sockets.send(conn, text);
      announce("joined");
    },
    message(_conn, data) {
      if (typeof data === "string") say(data);
    },
    close() {
      announce("left");
    },
  },

  fetch(req) {
    if (req.method === "POST") {
      const body = req.bytes();
      let text = "";
      try {
        text = body.length <= 4096 ? new TextDecoder("utf-8", { fatal: true }).decode(body).trim() : "";
      } catch {
        text = "";
      }
      if (!text) return { status: 400, body: "send a UTF-8 text body\n" };
      say(text);
      return "sent\n";
    }
    if (req.method === "GET") return `${recent(HISTORY).join("\n")}\n`;
    return { status: 405, body: "use GET or POST\n" };
  },
});
