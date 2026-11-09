// The app class of the jobs example, in Zig: an HTTP API over the Zig SDK's
// kv and queue clients, and the consumer the `jobs` queue pushes to. It
// talks to the Rust shard and queue classes of examples/jobs (`kv` and
// `queue`) and must be deployed as class `app`: `queue/jobs` delivers to
// `app/worker`. The routes and bodies are examples/ts-jobs's.
//
// Key-value (keys spread over 4 shards of class `kv`):
//
// - `GET /kv/<key>`: the value, with its version in `etag`; 404 if absent.
//   `?cached=1` reads through this actor's 60 s read cache.
// - `PUT /kv/<key>`: stores the body and answers the new version.
//   `if-match: <version>` or `if-none-match: *` make it a compare-and-set
//   (409 when it does not hold); `?ttl_ms=N` expires it.
// - `DELETE /kv/<key>`: 200 if it was there, 404 otherwise; with
//   `if-match: <version>`, only if it holds that version (409 otherwise).
// - `GET /kv?prefix=P`: the keys starting with `P`, one per line.
//
// Queues (actors of class `queue`):
//
// - `POST /jobs`: queues the body on `queue/jobs`, which pushes it to
//   `app/worker`. The worker stores the body in upper case at key
//   `job:<id>`; a body starting with `fail` asks for a retry every time, so
//   it ends in the dead-letter table after three attempts.
// - `GET /jobs/stats`, `GET /jobs/dead`: counts and dead letters.
// - `POST /inbox`: queues the body on `queue/inbox`, a pull queue.
// - `POST /inbox/batch`: queues each line of the body, in one write;
//   answers their ids, one per line.
// - `POST /inbox/lease?visibility_ms=N&max=M`: leases up to `M` (default
//   one) messages, answered as `<id> <attempt> <body>` lines.
// - `POST /inbox/ack?id=I&attempt=A`: 200 if that lease was current, 409
//   if it ran out (the message was leased again) or was settled.
//
// Workflows (instances of the Rust `orders` workflow class of
// examples/workflows, which calls its `ledger` class), as
// examples/workflows/app:
//
// - `POST /orders/<id>`: starts order `<id>` with the body as its input;
//   answers its status. Starting an id again answers the existing order's.
// - `GET /orders/<id>`: its status, one `key: value` per line; 404 if there
//   is none.
// - `POST /orders/<id>/approve`: delivers the `approved` event, then
//   answers the status (409 if the order has finished).
// - `POST /orders/<id>/cancel`: cancels it; answers its status.
const std = @import("std");
const wk = @import("wasmkeep");

const gpa = std.heap.c_allocator;
const shards = 4;

/// Keys over 4 shards, with a 60 s read cache: `get` may use it, writes
/// through this client update it, `getFresh` skips it.
const store = wk.kv.Client.init("kv", shards).withReadCache(60_000);
const jobs = wk.queue.Client.init("queue/jobs");
const inbox = wk.queue.Client.init("queue/inbox");
const orders = wk.workflow.Client.init("orders");

/// The value of `name` in the path's query (`?a=1&b=2`).
fn param(path: []const u8, name: []const u8) ?[]const u8 {
    const q = std.mem.indexOfScalar(u8, path, '?') orelse return null;
    var pairs = std.mem.splitScalar(u8, path[q + 1 ..], '&');
    while (pairs.next()) |pair| {
        var parts = std.mem.splitScalar(u8, pair, '=');
        const key = parts.next() orelse continue;
        // As the TypeScript app's split("="): up to the next '='.
        if (std.mem.eql(u8, key, name)) return parts.next() orelse "";
    }
    return null;
}

/// A non-negative integer, or null.
fn number(value: ?[]const u8) ?u64 {
    const s = value orelse return null;
    if (s.len == 0) return null;
    var v: u64 = 0;
    for (s) |ch| {
        if (ch < '0' or ch > '9') return null;
        v = std.math.mul(u64, v, 10) catch return null;
        v = std.math.add(u64, v, ch - '0') catch return null;
    }
    return v;
}

fn text(status: u16, body: []const u8) wk.Response {
    return wk.replyBytes(status, null, body);
}

fn kvFailure(err: wk.kv.Error) wk.Response {
    if (err == error.Conflict) return text(409, "the key changed\n");
    return wk.reply(502, "{s}\n", .{wk.kv.lastError()});
}

fn queueFailure(err: wk.queue.Error) wk.Response {
    // Every queue error answers 502 with its message, as the TypeScript app.
    switch (err) {
        error.Storage, error.Call, error.Refused => {},
    }
    return wk.reply(502, "{s}\n", .{wk.queue.lastError()});
}

fn withEtag(response: wk.Response, version: u64) wk.Response {
    var r = response;
    var buf: [20]u8 = undefined;
    wk.setHeader(&r, "etag", std.fmt.bufPrint(&buf, "{d}", .{version}) catch unreachable);
    return r;
}

fn kvRoute(req: *wk.Request, key: []const u8) !wk.Response {
    // Read if-match first: each header call reuses the C SDK's buffer.
    var version: ?u64 = null;
    if (req.header("if-match")) |match| {
        var digits: [32]u8 = undefined;
        var n: usize = 0;
        for (match) |ch| {
            if (ch == '"') continue;
            if (n == digits.len) return text(400, "if-match is not a version\n");
            digits[n] = ch;
            n += 1;
        }
        version = number(digits[0..n]) orelse return text(400, "if-match is not a version\n");
    }
    if (std.mem.eql(u8, req.method, "GET")) {
        const cached = if (param(req.path, "cached")) |v| std.mem.eql(u8, v, "1") else false;
        const result = if (cached) store.get(key) else store.getFresh(key);
        var entry = (result catch |e| return kvFailure(e)) orelse return text(404, "no such key\n");
        defer entry.deinit();
        return withEtag(wk.replyBytes(200, "application/octet-stream", entry.value), entry.version);
    }
    if (std.mem.eql(u8, req.method, "PUT")) {
        const absent = if (req.header("if-none-match")) |v| std.mem.eql(u8, v, "*") else false;
        const expect: wk.kv.Expect = if (version) |v| .{ .version = v } else if (absent) .absent else .any;
        const ttl_ms = number(param(req.path, "ttl_ms"));
        const v = store.putWith(key, req.body(), .{ .expect = expect, .ttl_ms = ttl_ms }) catch |e| return kvFailure(e);
        return withEtag(wk.reply(200, "{d}\n", .{v}), v);
    }
    if (std.mem.eql(u8, req.method, "DELETE")) {
        const result = if (version) |v| store.deleteIf(key, v) else store.delete(key);
        const removed = result catch |e| return kvFailure(e);
        return if (removed) text(200, "deleted\n") else text(404, "no such key\n");
    }
    return text(405, "use GET, PUT or DELETE\n");
}

fn list(path: []const u8) !wk.Response {
    var keys = store.list(param(path, "prefix") orelse "", null, 1000) catch |e| return kvFailure(e);
    defer keys.deinit();
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    for (0..keys.len()) |i| {
        try out.appendSlice(gpa, keys.at(i));
        try out.append(gpa, '\n');
    }
    return text(200, out.items);
}

fn send(q: *const wk.queue.Client, body: []const u8) wk.Response {
    const id = q.send(body) catch |e| return queueFailure(e);
    return wk.reply(200, "{d}\n", .{id});
}

fn sendBatch(body: []const u8) !wk.Response {
    var lines: std.ArrayList([]const u8) = .empty;
    defer lines.deinit(gpa);
    var it = std.mem.splitScalar(u8, body, '\n');
    while (it.next()) |line| {
        if (line.len > 0) try lines.append(gpa, line);
    }
    const ids = inbox.sendBatch(gpa, lines.items) catch |e| switch (e) {
        error.OutOfMemory => return e,
        else => |q| return queueFailure(q),
    };
    defer gpa.free(ids);
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    for (ids) |id| try out.print(gpa, "{d}\n", .{id});
    return text(200, out.items);
}

fn lease(path: []const u8) !wk.Response {
    const max = number(param(path, "max")) orelse 1;
    const visibility = number(param(path, "visibility_ms")) orelse 30_000;
    var messages = inbox.lease(@intCast(@min(max, std.math.maxInt(u32))), visibility) catch |e| return queueFailure(e);
    defer messages.deinit();
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    for (0..messages.len()) |i| {
        const m = messages.at(i);
        try out.print(gpa, "{d} {d} {s}\n", .{ m.id, m.attempt, m.body });
    }
    return text(200, out.items);
}

fn ack(path: []const u8) wk.Response {
    const id = number(param(path, "id"));
    const attempt = number(param(path, "attempt"));
    if (id == null or attempt == null or attempt.? > std.math.maxInt(u32)) return text(400, "give id and attempt\n");
    const current = inbox.ack(id.?, @intCast(attempt.?)) catch |e| return queueFailure(e);
    return if (current) text(200, "acked\n") else text(409, "that lease is no longer current\n");
}

fn stats(q: *const wk.queue.Client) wk.Response {
    const s = q.stats() catch |e| return queueFailure(e);
    return wk.reply(200, "ready={d} delayed={d} in_flight={d} dead={d}\n", .{ s.ready, s.delayed, s.in_flight, s.dead });
}

fn dead(q: *const wk.queue.Client) !wk.Response {
    var items = q.deadLetters(100) catch |e| return queueFailure(e);
    defer items.deinit();
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    for (0..items.len()) |i| {
        const d = items.at(i);
        try out.print(gpa, "{d} {d} {s}\n", .{ d.message.id, d.message.attempt, d.reason });
    }
    return text(200, out.items);
}

/// The worker: one outcome per pushed job. Writing `job:<id>` is
/// idempotent, so a duplicate delivery does no harm. (Upper case is ASCII
/// here; the TypeScript app's toUpperCase covers all of Unicode.)
fn work(queue: []const u8, message: wk.queue.Message) wk.queue.Outcome {
    _ = queue;
    if (std.mem.startsWith(u8, message.body, "fail")) return .retry;
    var key_buf: [32]u8 = undefined;
    const key = std.fmt.bufPrint(&key_buf, "job:{d}", .{message.id}) catch unreachable;
    const upper = gpa.alloc(u8, message.body.len) catch return .retry;
    defer gpa.free(upper);
    for (message.body, upper) |ch, *u| u.* = std.ascii.toUpper(ch);
    _ = store.put(key, upper) catch return .retry;
    return .ack;
}

/// A status as examples/workflows/app shows it: `key: value` lines.
fn show(status: *const wk.workflow.Status) !wk.Response {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(gpa);
    try out.print(gpa, "state: {s}\npositions: {d}\n", .{ @tagName(status.state), status.positions });
    if (status.wake_at) |at| try out.print(gpa, "wake_at: {d}\n", .{at});
    if (status.waitingCount() > 0) {
        try out.appendSlice(gpa, "waiting_for: ");
        for (0..status.waitingCount()) |i| {
            if (i > 0) try out.append(gpa, ',');
            try out.appendSlice(gpa, status.waitingFor(i));
        }
        try out.append(gpa, '\n');
    }
    if (status.output) |output| try out.print(gpa, "output: {s}\n", .{output});
    if (status.failure) |failure| try out.print(gpa, "error: {s}\n", .{failure});
    return text(200, out.items);
}

fn orderFailure(err: wk.workflow.Error) wk.Response {
    return switch (err) {
        error.Unknown => text(404, "no such order\n"),
        error.Finished => text(409, "the order has finished\n"),
        error.Call, error.Refused => wk.reply(502, "{s}\n", .{wk.workflow.lastError()}),
    };
}

/// `rest` is what follows `/orders/`: `<id>` or `<id>/<action>`.
fn orderRoute(req: *wk.Request, rest: []const u8) !wk.Response {
    const slash = std.mem.indexOfScalar(u8, rest, '/');
    const id = rest[0 .. slash orelse rest.len];
    const action: ?[]const u8 = if (slash) |s| rest[s + 1 ..] else null;
    if (id.len == 0) return text(404, "see the README for routes\n");
    const get = std.mem.eql(u8, req.method, "GET");
    const post = std.mem.eql(u8, req.method, "POST");
    const result = if (action == null and post)
        orders.start(id, req.body())
    else if (action == null and get)
        orders.status(id)
    else if (action != null and std.mem.eql(u8, action.?, "approve") and post) approve: {
        orders.sendEvent(id, "approved", "") catch |e| break :approve e;
        break :approve orders.status(id);
    } else if (action != null and std.mem.eql(u8, action.?, "cancel") and post)
        orders.cancel(id)
    else
        return text(404, "see the README for routes\n");
    var status = result catch |e| return orderFailure(e);
    defer status.deinit();
    return show(&status);
}

pub fn fetch(req: *wk.Request) !wk.Response {
    if (wk.queue.isDelivery(req)) return wk.queue.handleDelivery(req, work);
    const route = req.path[0 .. std.mem.indexOfScalar(u8, req.path, '?') orelse req.path.len];
    const get = std.mem.eql(u8, req.method, "GET");
    const post = std.mem.eql(u8, req.method, "POST");
    if (std.mem.eql(u8, route, "/kv") and get) return list(req.path);
    if (std.mem.startsWith(u8, route, "/kv/") and route.len > 4) return kvRoute(req, route[4..]);
    if (std.mem.eql(u8, route, "/jobs") and post) return send(&jobs, req.body());
    if (std.mem.eql(u8, route, "/jobs/stats") and get) return stats(&jobs);
    if (std.mem.eql(u8, route, "/jobs/dead") and get) return dead(&jobs);
    if (std.mem.eql(u8, route, "/inbox") and post) return send(&inbox, req.body());
    if (std.mem.eql(u8, route, "/inbox/batch") and post) return sendBatch(req.body());
    if (std.mem.eql(u8, route, "/inbox/stats") and get) return stats(&inbox);
    if (std.mem.eql(u8, route, "/inbox/lease") and post) return lease(req.path);
    if (std.mem.eql(u8, route, "/inbox/ack") and post) return ack(req.path);
    if (std.mem.startsWith(u8, route, "/orders/")) return orderRoute(req, route["/orders/".len..]);
    return text(404, "see the README for routes\n");
}

comptime {
    wk.actor(@This());
}
