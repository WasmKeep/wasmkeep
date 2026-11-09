// A counter actor in Zig, with the WasmKeep Zig SDK (sdks/zig). GET returns
// the value, POST increments it; each actor name is its own counter, kept in
// its SQLite database.
const std = @import("std");
const wk = @import("wasmkeep");

pub fn init() !void {
    _ = try wk.exec("CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)", .{});
    _ = try wk.exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)", .{});
}

pub fn fetch(req: *wk.Request) !wk.Response {
    const value = if (std.mem.eql(u8, req.method, "GET"))
        try wk.queryInt("SELECT value FROM counter WHERE id = 1", .{})
    else if (std.mem.eql(u8, req.method, "POST"))
        try wk.queryInt("UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value", .{})
    else
        return wk.reply(405, "use GET or POST\n", .{});
    return wk.reply(200, "{d}\n", .{value});
}

comptime {
    wk.actor(@This());
}
