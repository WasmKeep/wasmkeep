/++
A chat room in D (LDC, -betterC), with the WasmKeep D SDK (sdks/d). Each
actor name is one room: the Rust examples/chat-room, line for line in
behaviour.

- Connect a WebSocket to /actor/<class>/<room>: you receive the last 20
  messages, then everything said in the room. Text you send is broadcast
  to everyone in the room, you included. If the operator set MOTD
  (wasmkeep serve --env chat=FILE), it comes first, as "* <MOTD>".
- POST / with a text body says something over plain HTTP.
- GET / lists the recent messages.

Messages are kept in the room's SQLite database (the last 100). The host
owns the sockets, so the room can hibernate while people stay connected:
the next message wakes it.
+/
module chat_room;

import core.stdc.string : memcpy;

import wasmkeep;

enum history = 20;
enum keep = 100;
enum recent = "SELECT text FROM (SELECT id, text FROM messages ORDER BY id DESC LIMIT ?) ORDER BY id";

void say(const(char)[] text) {
    exec("INSERT INTO messages (at, text) VALUES (?, ?)", nowMs(), text);
    exec("DELETE FROM messages WHERE id <= (SELECT max(id) FROM messages) - ?", keep);
    broadcast(text);
}

/// "* someone joined #lobby (2 online)": the room is this actor's name.
void announce(const(char)[] what) {
    auto name = self().name;
    auto r = reply(200, "* someone %.*s #%.*s (%zu online)", cast(int) what.length, what.ptr,
        cast(int) name.length, name.ptr, connections().length);
    broadcast(cast(const(char)[]) r.body);
}

/// Whether b is valid UTF-8 (no overlong forms, surrogates or code points
/// past U+10FFFF).
bool utf8(const(ubyte)[] s) {
    size_t i = 0;
    while (i < s.length) {
        ubyte c = s[i];
        size_t n;
        ubyte lo = 0x80, hi = 0xBF;
        if (c < 0x80) n = 0;
        else if (c >= 0xC2 && c <= 0xDF) n = 1;
        else if (c >= 0xE0 && c <= 0xEF) { n = 2; lo = c == 0xE0 ? 0xA0 : 0x80; hi = c == 0xED ? 0x9F : 0xBF; }
        else if (c >= 0xF0 && c <= 0xF4) { n = 3; lo = c == 0xF0 ? 0x90 : 0x80; hi = c == 0xF4 ? 0x8F : 0xBF; }
        else return false;
        if (n > s.length - i - 1) return false;
        if (n && (s[i + 1] < lo || s[i + 1] > hi)) return false;
        foreach (k; 2 .. n + 1) if ((s[i + k] & 0xC0) != 0x80) return false;
        i += n + 1;
    }
    return true;
}

bool space(ubyte c) => c == ' ' || (c >= '\t' && c <= '\r');

struct Room {
    static void init() {
        if (exec("CREATE TABLE IF NOT EXISTS messages"
                ~ " (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, text TEXT NOT NULL)") < 0)
            fail(eventCopy(lastError()));
    }

    static bool open(ulong conn, const(char)[] path) {
        auto motd = env("MOTD");
        if (motd !is null) {
            auto r = reply(200, "* %.*s", cast(int) motd.length, motd.ptr);
            send(conn, cast(const(char)[]) r.body);
        }
        auto rows = query(recent, history);
        foreach (i; 0 .. rows.count) send(conn, rows.getText(i, 0));
        announce("joined");
        return true;
    }

    static void message(ulong conn, const(ubyte)[] data, bool binary) {
        if (!binary) say(cast(const(char)[]) data);
    }

    static void close(ulong conn, ushort code, const(char)[] reason) {
        announce("left");
    }

    static Response fetch(ref Request req) {
        if (req.method == "POST") {
            auto b = req.body();
            if (b.length > 4096 || !utf8(b)) b = null;
            while (b.length && space(b[0])) b = b[1 .. $];
            while (b.length && space(b[$ - 1])) b = b[0 .. $ - 1];
            if (!b.length) return replyText(400, "send a UTF-8 text body\n");
            say(cast(const(char)[]) b);
            return replyText(200, "sent\n");
        }
        if (req.method == "GET") {
            auto rows = query(recent, history);
            if (!rows.ok) return reply(500, "%.*s\n", cast(int) lastError().length, lastError().ptr);
            size_t total = 1;
            foreach (i; 0 .. rows.count) total += rows.getText(i, 0).length + 1;
            auto out_ = cast(char*) eventAlloc(total);
            size_t at = 0;
            foreach (i; 0 .. rows.count) {
                auto t = rows.getText(i, 0);
                if (t.length) memcpy(out_ + at, t.ptr, t.length);
                at += t.length;
                if (i + 1 < rows.count) out_[at++] = '\n';
            }
            out_[at++] = '\n';
            return replyText(200, out_[0 .. at]);
        }
        return replyText(405, "use GET or POST\n");
    }
}

mixin Actor!Room;
