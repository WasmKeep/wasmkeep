// A chat room in Zig, with the WasmKeep Zig SDK (sdks/zig). Each actor name is
// one room: the Rust examples/chat-room, line for line in behaviour.
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
const std = @import("std");
const wk = @import("wasmkeep");

const history = 20;
const keep = 100;
const recent_sql = "SELECT text FROM (SELECT id, text FROM messages ORDER BY id DESC LIMIT ?) ORDER BY id";

fn say(text: []const u8) !void {
    _ = try wk.exec("INSERT INTO messages (at, text) VALUES (?, ?)", .{ wk.nowMs(), text });
    _ = try wk.exec("DELETE FROM messages WHERE id <= (SELECT max(id) FROM messages) - ?", .{keep});
    // A slow reader drops this message; the others still get it.
    wk.broadcast(text);
}

/// "* someone joined #lobby (2 online)": the room is this actor's name.
fn announce(what: []const u8) void {
    var buf: [256]u8 = undefined;
    const room = wk.selfAddress().name;
    const line = std.fmt.bufPrint(&buf, "* someone {s} #{s} ({d} online)", .{ what, room, wk.connectionCount() }) catch
        std.fmt.bufPrint(&buf, "* someone {s} ({d} online)", .{ what, wk.connectionCount() }) catch return;
    wk.broadcast(line);
}

pub fn init() !void {
    _ = try wk.exec("CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, text TEXT NOT NULL)", .{});
}

pub fn wsOpen(conn: u64, path: []const u8) bool {
    _ = path;
    if (wk.env("MOTD")) |motd| {
        var buf: [512]u8 = undefined;
        // A long MOTD is cut short.
        const line = std.fmt.bufPrint(&buf, "* {s}", .{motd[0..@min(motd.len, buf.len - 2)]}) catch unreachable;
        _ = wk.send(conn, line);
    }
    if (wk.query(recent_sql, .{history})) |rows_| {
        var rows = rows_;
        defer rows.deinit();
        for (0..rows.len()) |i| _ = wk.send(conn, rows.text(i, 0));
    } else |_| {}
    announce("joined");
    return true;
}

pub fn wsMessage(conn: u64, data: []const u8, binary: bool) void {
    _ = conn;
    if (!binary) say(data) catch {};
}

pub fn wsClose(conn: u64, code: u16, reason: []const u8) void {
    _ = .{ conn, code, reason };
    announce("left");
}

pub fn fetch(req: *wk.Request) !wk.Response {
    if (std.mem.eql(u8, req.method, "POST")) {
        const body = req.body();
        const text = if (body.len <= 4096 and std.unicode.utf8ValidateSlice(body))
            std.mem.trim(u8, body, " \t\r\n")
        else
            "";
        if (text.len == 0) return wk.reply(400, "send a UTF-8 text body\n", .{});
        try say(text);
        return wk.reply(200, "sent\n", .{});
    }
    if (std.mem.eql(u8, req.method, "GET")) {
        var rows = try wk.query(recent_sql, .{history});
        defer rows.deinit();
        var total: usize = 1;
        for (0..rows.len()) |i| total += rows.text(i, 0).len + 1;
        const out = try std.heap.c_allocator.alloc(u8, total);
        defer std.heap.c_allocator.free(out);
        var at: usize = 0;
        for (0..rows.len()) |i| {
            const t = rows.text(i, 0);
            @memcpy(out[at..][0..t.len], t);
            at += t.len;
            if (i + 1 < rows.len()) {
                out[at] = '\n';
                at += 1;
            }
        }
        out[at] = '\n';
        return wk.replyBytes(200, null, out[0 .. at + 1]);
    }
    return wk.reply(405, "use GET or POST\n", .{});
}

comptime {
    wk.actor(@This());
}
