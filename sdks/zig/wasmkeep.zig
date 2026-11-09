//! The WasmKeep Zig SDK: write actors as a Zig file with a few public
//! functions.
//!
//! ```zig
//! const wk = @import("wasmkeep");
//!
//! pub fn init() !void {
//!     _ = try wk.exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)", .{});
//! }
//!
//! pub fn fetch(req: *wk.Request) !wk.Response {
//!     _ = try wk.exec("INSERT INTO hits (at) VALUES (?)", .{wk.nowMs()});
//!     const n = try wk.queryInt("SELECT count(*) FROM hits", .{});
//!     return wk.reply(200, "{d} hits on {s}\n", .{ n, req.path });
//! }
//!
//! comptime {
//!     wk.actor(@This());
//! }
//! ```
//!
//! Hooks, all optional: `init`, `fetch`, `alarm`, `wsOpen`, `wsMessage`,
//! `wsClose` (signatures below, at `actor`). A hook may return an error: in
//! `init` and `alarm` it fails the event (an alarm is retried), in `fetch` it
//! becomes a 500. Every hook runs one event at a time per actor: no locks
//! needed.
//!
//! Key-value and queue clients (ADR 0015): `wk.kv` and `wk.queue`, below;
//! the workflow client (ADR 0019): `wk.workflow`.
//!
//! This wraps the C SDK (sdks/c); see examples/zig-counter/build.sh for the
//! build.
const std = @import("std");

/// The C SDK (`wasmkeep.h`), translated.
pub const c = @import("wasmkeep_c");

pub const Error = error{ Sql, Call };

/// A blob parameter (a plain `[]const u8` binds as text).
pub const Blob = struct { bytes: []const u8 };

fn toValue(v: anytype) c.wk_value {
    const T = @TypeOf(v);
    if (T == Blob) return c.wk_blob(v.bytes.ptr, v.bytes.len);
    switch (@typeInfo(T)) {
        .null => return c.wk_null(),
        .bool => return c.wk_int(if (v) 1 else 0),
        .int, .comptime_int => return c.wk_int(@intCast(v)),
        .float, .comptime_float => return c.wk_real(@floatCast(v)),
        .optional => return if (v) |x| toValue(x) else c.wk_null(),
        .pointer => |p| {
            if (p.size == .slice and p.child == u8) return c.wk_text_n(v.ptr, v.len);
            if (p.size == .one) {
                const child = @typeInfo(p.child);
                if (child == .array and child.array.child == u8) return c.wk_text_n(v, child.array.len);
            }
        },
        else => {},
    }
    @compileError("cannot bind " ++ @typeName(T) ++ " to SQL");
}

fn Values(comptime T: type) type {
    return [@typeInfo(T).@"struct".field_names.len]c.wk_value;
}

fn toValues(params: anytype) Values(@TypeOf(params)) {
    var out: Values(@TypeOf(params)) = undefined;
    inline for (params, 0..) |p, i| out[i] = toValue(p);
    return out;
}

/// The last failure's message (SQL or call).
pub fn lastError() []const u8 {
    return std.mem.span(c.wk_error());
}

/// The last failure's kind: syntax, constraint, ... for SQL; timeout, ...
/// for calls.
pub fn lastErrorKind() []const u8 {
    return std.mem.span(c.wk_error_kind());
}

// ------------------------------------------------------------- storage

/// Runs a statement that returns no rows; returns the rows changed.
/// `params` is a tuple: `.{ 42, "text", null, wk.Blob{ .bytes = b } }`.
pub fn exec(sql: [*:0]const u8, params: anytype) Error!i64 {
    const values = toValues(params);
    const n = c.wk_exec_v(sql, &values, values.len);
    return if (n < 0) error.Sql else n;
}

/// Like `exec`, but returns only once the write is durable (replicated to
/// the cluster's bucket, or fsynced on a single node). Not inside a
/// transaction.
pub fn execDurable(sql: [*:0]const u8, params: anytype) Error!i64 {
    const values = toValues(params);
    const n = c.wk_exec_durable_v(sql, &values, values.len);
    return if (n < 0) error.Sql else n;
}

/// Query results; call `deinit` when done (text read from them is valid
/// until then).
pub const Rows = struct {
    raw: c.wk_rows,

    pub fn len(self: *const Rows) usize {
        return self.raw.count;
    }

    /// A column's index by name.
    pub fn column(self: *const Rows, name: [*:0]const u8) ?usize {
        const i = c.wk_column(&self.raw, name);
        return if (i < 0) null else @intCast(i);
    }

    pub fn get(self: *const Rows, row: usize, col: usize) c.wk_value {
        return c.wk_get(&self.raw, row, col);
    }

    pub fn int(self: *const Rows, row: usize, col: usize) i64 {
        const v = self.get(row, col);
        return switch (v.type) {
            c.WK_INTEGER => v.as.integer,
            c.WK_REAL => @intFromFloat(v.as.real),
            else => 0,
        };
    }

    pub fn real(self: *const Rows, row: usize, col: usize) f64 {
        const v = self.get(row, col);
        return switch (v.type) {
            c.WK_INTEGER => @floatFromInt(v.as.integer),
            c.WK_REAL => v.as.real,
            else => 0,
        };
    }

    /// Text or blob bytes; empty for other types.
    pub fn text(self: *const Rows, row: usize, col: usize) []const u8 {
        const v = self.get(row, col);
        if (v.type != c.WK_TEXT and v.type != c.WK_BLOB) return "";
        return v.as.bytes.ptr[0..v.as.bytes.len];
    }

    pub fn deinit(self: *Rows) void {
        c.wk_rows_free(&self.raw);
    }
};

/// Runs a query.
pub fn query(sql: [*:0]const u8, params: anytype) Error!Rows {
    const values = toValues(params);
    var rows: Rows = undefined;
    if (!c.wk_query_v(&rows.raw, sql, &values, values.len)) return error.Sql;
    return rows;
}

/// The first column of the first row as an integer (0 if there is no row).
pub fn queryInt(sql: [*:0]const u8, params: anytype) Error!i64 {
    var rows = try query(sql, params);
    defer rows.deinit();
    return rows.int(0, 0);
}

/// Deletes this actor once the current event ends.
pub fn deleteSelf() void {
    c.wk_delete_self();
}

/// An actor's address. Within a tenant, `class/name` is what `call` takes
/// (the name is not encoded).
pub const Address = struct {
    tenant: []const u8,
    class: []const u8,
    name: []const u8,
};

/// This actor's own address. The slices live as long as the instance.
pub fn selfAddress() Address {
    const a = c.wk_self_address();
    return .{
        .tenant = std.mem.span(a.tenant),
        .class = std.mem.span(a.class_name),
        .name = std.mem.span(a.name),
    };
}

/// Configuration and secrets the operator gives this class (`wasmkeep serve
/// --env CLASS=FILE`): the value of `name`, or null if it is not set. The
/// slice lives as long as the instance.
pub fn env(name: [*:0]const u8) ?[]const u8 {
    const v = c.wk_env(name) orelse return null;
    return std.mem.span(v);
}

/// Wall-clock time, in ms since the Unix epoch.
pub fn nowMs() i64 {
    return c.wk_now_ms();
}

// -------------------------------------------------------------- alarms

/// Sets (replaces) the actor's single alarm, in ms since the Unix epoch.
/// It fires `alarm` at least once, also after a crash or a move.
pub fn setAlarm(at_ms: i64) void {
    c.wk_alarm_set(at_ms);
}

pub fn getAlarm() ?i64 {
    var at: i64 = 0;
    return if (c.wk_alarm_get(&at)) at else null;
}

pub fn clearAlarm() void {
    c.wk_alarm_clear();
}

// ---------------------------------------------------------- WebSockets

/// Sends a text message. False if the connection is gone or its buffer is
/// full.
pub fn send(conn: u64, text: []const u8) bool {
    return c.wk_send_n(conn, text.ptr, text.len);
}

pub fn sendBinary(conn: u64, data: []const u8) bool {
    return c.wk_send_binary(conn, data.ptr, data.len);
}

/// Sends text to every open connection of this actor.
pub fn broadcast(text: []const u8) void {
    c.wk_broadcast_n(text.ptr, text.len);
}

pub fn close(conn: u64, code: u16, reason: [*:0]const u8) void {
    c.wk_close(conn, code, reason);
}

/// How many connections this actor has open.
pub fn connectionCount() usize {
    return c.wk_connections(null, 0);
}

/// Fills `out` with open connection ids; returns those written.
pub fn connections(out: []u64) []u64 {
    const n = c.wk_connections(out.ptr, out.len);
    return out[0..@min(n, out.len)];
}

// --------------------------------------------------------- actor calls

/// Another actor's answer; call `deinit` when done.
pub const CallResponse = struct {
    raw: c.wk_call_response,

    pub fn status(self: *const CallResponse) u16 {
        return self.raw.status;
    }

    pub fn body(self: *const CallResponse) []const u8 {
        return self.raw.body.ptr[0..self.raw.body.len];
    }

    pub fn deinit(self: *CallResponse) void {
        c.wk_call_response_free(&self.raw);
    }
};

/// Calls actor "class/name" (same tenant) and waits for its response.
pub fn call(address: [*:0]const u8, method: [*:0]const u8, path: [*:0]const u8, body: []const u8) Error!CallResponse {
    var r: CallResponse = undefined;
    if (!c.wk_call(address, method, path, body.ptr, body.len, &r.raw)) return error.Call;
    return r;
}

// ---------------------------------------------------------------- HTTP

pub const Request = struct {
    raw: *c.wk_request,
    /// "GET", "POST", ...
    method: []const u8,
    /// With the query, relative to the actor.
    path: []const u8,

    /// The body, read on first use.
    pub fn body(self: *Request) []const u8 {
        const b = c.wk_request_body(self.raw);
        return b.ptr[0..b.len];
    }

    /// A header's value (names are lowercase); valid until the next call.
    pub fn header(self: *Request, name: [*:0]const u8) ?[]const u8 {
        const v = c.wk_request_header(self.raw, name) orelse return null;
        return std.mem.span(v);
    }
};

pub const Response = c.wk_response;

/// A text response, formatted with `std.fmt`.
pub fn reply(status: u16, comptime fmt: []const u8, args: anytype) Response {
    var buf: [4096]u8 = undefined;
    if (std.fmt.bufPrint(&buf, fmt, args)) |text| {
        return c.wk_reply_copy(status, null, text.ptr, text.len);
    } else |_| {
        const text = std.fmt.allocPrint(std.heap.c_allocator, fmt, args) catch
            return c.wk_reply_static(500, null, "out of memory\n", 14);
        defer std.heap.c_allocator.free(text);
        return c.wk_reply_copy(status, null, text.ptr, text.len);
    }
}

/// A response with the given content type; the body is copied.
pub fn replyBytes(status: u16, content_type: ?[*:0]const u8, body: []const u8) Response {
    return c.wk_reply_copy(status, content_type, body.ptr, body.len);
}

/// Adds a header to a response (`etag`, `cache-control`, ...); name and
/// value are copied.
pub fn setHeader(response: *Response, name: [*:0]const u8, value: []const u8) void {
    c.wk_response_header_n(response, name, value.ptr, value.len);
}

// ------------------------------------------------- key-value and queues

/// Key-value storage built from actors (ADR 0015): `wk.kv.Client`. See
/// kv.zig.
pub const kv = @import("kv.zig");

/// Queues with at-least-once delivery (ADR 0015): `wk.queue.Client` and
/// `wk.queue.handleDelivery`. See queue.zig.
pub const queue = @import("queue.zig");

// ----------------------------------------------------------- workflows

/// Durable workflows (ADR 0019), the client side: `wk.workflow.Client`
/// starts, queries, signals and cancels instances of a workflow class. See
/// workflow.zig.
pub const workflow = @import("workflow.zig");

// --------------------------------------------------------------- hooks

fn failed(err: anyerror) void {
    c.wk_fail(@errorName(err).ptr);
}

fn ReturnType(comptime f: anytype) type {
    return @typeInfo(@TypeOf(f)).@"fn".return_type.?;
}

fn isErrorUnion(comptime T: type) bool {
    return @typeInfo(T) == .error_union;
}

/// Exports the hooks `T` declares (the others keep their defaults):
///
/// - `pub fn init() void` (or `!void`): once per component version, before
///   any other event; create or migrate tables here.
/// - `pub fn fetch(req: *wk.Request) wk.Response` (or `!wk.Response`).
/// - `pub fn alarm() void` (or `!void`): the alarm fired.
/// - `pub fn wsOpen(conn: u64, path: []const u8) bool`: false refuses it.
/// - `pub fn wsMessage(conn: u64, data: []const u8, binary: bool) void`.
/// - `pub fn wsClose(conn: u64, code: u16, reason: []const u8) void`.
pub fn actor(comptime T: type) void {
    if (@hasDecl(T, "init")) {
        const S = struct {
            fn hook() callconv(.c) void {
                if (comptime isErrorUnion(ReturnType(T.init))) T.init() catch |e| failed(e) else T.init();
            }
        };
        @export(&S.hook, .{ .name = "wk_init" });
    }
    if (@hasDecl(T, "alarm")) {
        const S = struct {
            fn hook() callconv(.c) void {
                if (comptime isErrorUnion(ReturnType(T.alarm))) T.alarm() catch |e| failed(e) else T.alarm();
            }
        };
        @export(&S.hook, .{ .name = "wk_alarm" });
    }
    if (@hasDecl(T, "fetch")) {
        const S = struct {
            fn hook(raw: [*c]c.wk_request) callconv(.c) Response {
                var req = Request{
                    .raw = raw,
                    .method = std.mem.span(raw.*.method),
                    .path = std.mem.span(raw.*.path),
                };
                if (comptime isErrorUnion(ReturnType(T.fetch))) {
                    return T.fetch(&req) catch |e| reply(500, "{s}\n", .{@errorName(e)});
                }
                return T.fetch(&req);
            }
        };
        @export(&S.hook, .{ .name = "wk_fetch" });
    }
    if (@hasDecl(T, "wsOpen")) {
        const S = struct {
            fn hook(conn: u64, path: [*c]const u8) callconv(.c) bool {
                return T.wsOpen(conn, std.mem.span(path));
            }
        };
        @export(&S.hook, .{ .name = "wk_ws_open" });
    }
    if (@hasDecl(T, "wsMessage")) {
        const S = struct {
            fn hook(conn: u64, data: c.wk_bytes, binary: bool) callconv(.c) void {
                T.wsMessage(conn, data.ptr[0..data.len], binary);
            }
        };
        @export(&S.hook, .{ .name = "wk_ws_message" });
    }
    if (@hasDecl(T, "wsClose")) {
        const S = struct {
            fn hook(conn: u64, code: u16, reason: [*c]const u8) callconv(.c) void {
                T.wsClose(conn, code, std.mem.span(reason));
            }
        };
        @export(&S.hook, .{ .name = "wk_ws_close" });
    }
}
