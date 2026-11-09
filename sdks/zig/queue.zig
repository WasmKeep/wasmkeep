//! Queues with at-least-once delivery, built from one actor (ADR 0015): the
//! producer and consumer side, over the C SDK's. The queue class is the
//! Rust SDK's (examples/jobs/queue).
//!
//! ```zig
//! const jobs = wk.queue.Client.init("queue/jobs");
//! const id = try jobs.send("resize 42");
//!
//! fn work(queue: []const u8, m: wk.queue.Message) wk.queue.Outcome {
//!     ...
//!     return .ack;
//! }
//!
//! pub fn fetch(req: *wk.Request) !wk.Response {
//!     if (wk.queue.isDelivery(req)) return wk.queue.handleDelivery(req, work);
//!     ...
//! }
//! ```
const std = @import("std");
const c = @import("wasmkeep_c");
const wk = @import("wasmkeep.zig");
const Request = wk.Request;
const Response = wk.Response;

fn toBytes(s: []const u8) c.wk_bytes {
    return .{ .ptr = s.ptr, .len = s.len };
}

fn fromBytes(b: c.wk_bytes) []const u8 {
    return if (b.len == 0) "" else b.ptr[0..b.len];
}

/// Messages one send may carry.
pub const max_batch: usize = c.WK_QUEUE_MAX_BATCH;
/// The path a push delivery arrives on, at the consumer.
pub const deliver_path = c.WK_QUEUE_DELIVER_PATH;

/// Why an operation failed; `lastError()` and `lastStatus()` say more.
/// - `Storage`: the queue's database refused the operation;
/// - `Call`: the call to the queue failed; a send may have applied;
/// - `Refused`: the queue answered with an unexpected status.
pub const Error = error{ Storage, Call, Refused };

var last: c.wk_queue_error = .{};

/// The last failure's message, worded as in the Rust SDK.
pub fn lastError() []const u8 {
    return std.mem.sliceTo(&last.message, 0);
}

/// The queue's status for the last `error.Refused` (200: malformed).
pub fn lastStatus() u16 {
    return last.status;
}

fn failed() Error {
    return switch (last.kind) {
        c.WK_QUEUE_STORAGE => error.Storage,
        c.WK_QUEUE_CALL => error.Call,
        else => error.Refused,
    };
}

/// A leased or delivered message; `body` lives as long as what holds it.
pub const Message = struct {
    /// Unique within the queue, the same on every delivery.
    id: u64,
    /// Which delivery this is, from 1.
    attempt: u32,
    /// When it was sent (ms since the Unix epoch).
    enqueued_at: u64,
    body: []const u8,

    fn fromC(m: c.wk_queue_message) Message {
        return .{ .id = m.id, .attempt = m.attempt, .enqueued_at = m.enqueued_at, .body = fromBytes(m.body) };
    }
};

/// What a consumer did with a message: `.ack` (delete it), `.retry`
/// (after the queue's backoff) or `.{ .retry_after = ms }`.
pub const Outcome = union(enum) {
    ack,
    retry,
    retry_after: u64,

    fn toC(self: Outcome) c.wk_queue_outcome {
        return switch (self) {
            .ack => c.wk_queue_ack_outcome(),
            .retry => c.wk_queue_retry_outcome(),
            .retry_after => |ms| c.wk_queue_retry_after(ms),
        };
    }
};

/// One settlement: a lease (`id`, `attempt`) and its outcome.
pub const Settlement = struct { id: u64, attempt: u32, outcome: Outcome };

/// How many messages are where.
pub const Stats = struct { ready: u64, delayed: u64, in_flight: u64, dead: u64 };

/// Messages from `Client.lease`; call `deinit` when done.
pub const Messages = struct {
    raw: c.wk_queue_messages,

    pub fn len(self: *const Messages) usize {
        return self.raw.count;
    }

    pub fn at(self: *const Messages, i: usize) Message {
        return Message.fromC(self.raw.items[i]);
    }

    pub fn deinit(self: *Messages) void {
        c.wk_queue_messages_free(&self.raw);
    }
};

/// A dead-lettered message.
pub const DeadLetter = struct {
    message: Message,
    /// When it was dead-lettered (ms since the Unix epoch).
    failed_at: u64,
    /// Why (UTF-8).
    reason: []const u8,
};

/// Dead letters from `Client.deadLetters`; call `deinit` when done.
pub const DeadLetters = struct {
    raw: c.wk_queue_dead_letters,

    pub fn len(self: *const DeadLetters) usize {
        return self.raw.count;
    }

    pub fn at(self: *const DeadLetters, i: usize) DeadLetter {
        const d = self.raw.items[i];
        return .{ .message = Message.fromC(d.message), .failed_at = d.failed_at, .reason = fromBytes(d.reason) };
    }

    pub fn deinit(self: *DeadLetters) void {
        c.wk_queue_dead_letters_free(&self.raw);
    }
};

/// Reaches one queue actor: a plain value, copy it freely.
pub const Client = struct {
    raw: c.wk_queue,

    /// The queue at `address` ("class/name", borrowed). Usable at
    /// comptime.
    pub fn init(address: [*:0]const u8) Client {
        return .{ .raw = .{ .address = address } };
    }

    /// Queues `body`; returns its id once it is durable.
    pub fn send(self: *const Client, body: []const u8) Error!u64 {
        return self.sendDelayed(body, 0);
    }

    /// Queues `body`, visible after `delay_ms`.
    pub fn sendDelayed(self: *const Client, body: []const u8, delay_ms: u64) Error!u64 {
        var id: u64 = 0;
        if (!c.wk_queue_send_delayed(&self.raw, toBytes(body), delay_ms, &id, &last)) return failed();
        return id;
    }

    /// Queues several messages (at most `max_batch`) in one durable
    /// write; returns their ids in order (free them with `allocator`).
    /// All or none are queued.
    pub fn sendBatch(self: *const Client, allocator: std.mem.Allocator, bodies: []const []const u8) (Error || std.mem.Allocator.Error)![]u64 {
        const raw = try allocator.alloc(c.wk_bytes, bodies.len);
        defer allocator.free(raw);
        for (bodies, raw) |b, *r| r.* = toBytes(b);
        const ids = try allocator.alloc(u64, bodies.len);
        errdefer allocator.free(ids);
        if (!c.wk_queue_send_batch(&self.raw, raw.ptr, raw.len, ids.ptr, &last)) return failed();
        return ids;
    }

    /// Leases up to `max` visible messages for `visibility_ms`.
    pub fn lease(self: *const Client, max: u32, visibility_ms: u64) Error!Messages {
        var out: Messages = .{ .raw = .{} };
        if (!c.wk_queue_lease(&self.raw, max, visibility_ms, &out.raw, &last)) return failed();
        return out;
    }

    /// Deletes a leased message; false if this lease is no longer
    /// current (it ran out and the message was leased again, or was
    /// settled).
    pub fn ack(self: *const Client, id: u64, attempt: u32) Error!bool {
        var current = false;
        if (!c.wk_queue_ack(&self.raw, id, attempt, &current, &last)) return failed();
        return current;
    }

    /// Makes a leased message visible again after `delay_ms` (null: the
    /// queue's backoff); false if this lease is no longer current.
    pub fn retry(self: *const Client, id: u64, attempt: u32, delay_ms: ?u64) Error!bool {
        var current = false;
        const delay: u64 = delay_ms orelse 0;
        const delay_ptr: [*c]const u64 = if (delay_ms != null) &delay else null;
        if (!c.wk_queue_retry(&self.raw, id, attempt, delay_ptr, &current, &last)) return failed();
        return current;
    }

    /// Settles several leased messages in one call; one flag per item
    /// (free them with `allocator`), true where the lease was current.
    pub fn settle(self: *const Client, allocator: std.mem.Allocator, items: []const Settlement) (Error || std.mem.Allocator.Error)![]bool {
        const raw = try allocator.alloc(c.wk_queue_settlement, items.len);
        defer allocator.free(raw);
        for (items, raw) |s, *r| r.* = .{ .id = s.id, .attempt = s.attempt, .outcome = s.outcome.toC() };
        const flags = try allocator.alloc(bool, items.len);
        errdefer allocator.free(flags);
        if (!c.wk_queue_settle(&self.raw, raw.ptr, raw.len, flags.ptr, &last)) return failed();
        return flags;
    }

    /// How many messages are ready, delayed, in flight and dead.
    pub fn stats(self: *const Client) Error!Stats {
        var s: c.wk_queue_stats = .{};
        if (!c.wk_queue_get_stats(&self.raw, &s, &last)) return failed();
        return .{ .ready = s.ready, .delayed = s.delayed, .in_flight = s.in_flight, .dead = s.dead };
    }

    /// The oldest `limit` dead-lettered messages.
    pub fn deadLetters(self: *const Client, limit: u32) Error!DeadLetters {
        var out: DeadLetters = .{ .raw = .{} };
        if (!c.wk_queue_get_dead_letters(&self.raw, limit, &out.raw, &last)) return failed();
        return out;
    }
};

/// Whether `req` is a push delivery from a queue (answer it with
/// `handleDelivery`).
pub fn isDelivery(req: *Request) bool {
    return c.wk_queue_is_delivery(req.raw);
}

/// Answers a push delivery: `handler(queue, message)` is called once per
/// message and returns its `Outcome` (or an error union of one). Return
/// the response from `fetch`. Deliveries from outside clients (without
/// `x-wasmkeep-caller`) get 403, a malformed body 400. A handler that
/// returns an error stops the delivery: the error is returned (so
/// `fetch` answers 500) and the queue retries the whole batch with its
/// backoff.
pub fn handleDelivery(req: *Request, comptime handler: anytype) anyerror!Response {
    const S = struct {
        var failure: ?anyerror = null;

        fn each(q: [*c]const u8, m: [*c]const c.wk_queue_message, outcome: [*c]c.wk_queue_outcome, ctx: ?*anyopaque) callconv(.c) bool {
            _ = ctx;
            const result = handler(std.mem.span(q), Message.fromC(m.*));
            const o: Outcome = if (comptime @typeInfo(@TypeOf(result)) == .error_union)
                result catch |e| {
                    failure = e;
                    return false;
                }
            else
                result;
            outcome.* = o.toC();
            return true;
        }
    };
    S.failure = null;
    const r = c.wk_queue_handle_delivery(req.raw, S.each, null);
    if (S.failure) |e| return e;
    return r;
}
