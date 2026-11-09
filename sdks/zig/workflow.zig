//! Durable workflows built from actors (ADR 0019): the client side, over
//! the C SDK's (`wasmkeep_workflow.h`). Each instance is the actor
//! `<class>/<id>` of a workflow class run by the Rust (or TypeScript) SDK's
//! runner (examples/workflows: `orders`).
//!
//! ```zig
//! const orders = wk.workflow.Client.init("orders");
//! var status = try orders.start("order-42", "item=book"); // idempotent while kept
//! defer status.deinit();
//! try orders.sendEvent("order-42", "approved", "");
//! var now = try orders.status("order-42"); // error.Unknown if there is none
//! defer now.deinit();
//! // now.state, now.wake_at, now.waitingFor(i), now.output, now.failure
//! ```
const std = @import("std");
const c = @import("wasmkeep_c");

fn toBytes(s: []const u8) c.wk_bytes {
    return .{ .ptr = s.ptr, .len = s.len };
}

fn fromBytes(b: c.wk_bytes) []const u8 {
    return if (b.len == 0) "" else b.ptr[0..b.len];
}

/// Why an operation failed; `lastError()` and `lastStatus()` say more.
/// - `Unknown`: no instance with that id (never started, or deleted after
///   its retention);
/// - `Finished`: the instance has finished, so it takes no more events;
/// - `Call`: the call failed; the instance may or may not have recorded it;
/// - `Refused`: the instance answered with an unexpected status (200: a
///   malformed status).
pub const Error = error{ Unknown, Finished, Call, Refused };

var last: c.wk_workflow_error = .{};

/// The last failure's message, worded as in the Rust SDK (valid until
/// the next failure).
pub fn lastError() []const u8 {
    return std.mem.sliceTo(&last.message, 0);
}

/// The instance's status for the last `error.Refused` (200: a malformed
/// status).
pub fn lastStatus() u16 {
    return last.status;
}

fn failed() Error {
    return switch (last.kind) {
        c.WK_WORKFLOW_UNKNOWN => error.Unknown,
        c.WK_WORKFLOW_FINISHED => error.Finished,
        c.WK_WORKFLOW_CALL => error.Call,
        else => error.Refused,
    };
}

/// Where an instance stands.
pub const State = enum {
    /// Started, or continuing: its alarm runs it next.
    running,
    /// Waiting for a time or an event.
    waiting,
    /// Returned its output.
    completed,
    /// Failed with `Status.failure`.
    failed,
    /// Cancelled with `Client.cancel`.
    cancelled,

    fn fromC(state: c.wk_workflow_state) State {
        return switch (state) {
            c.WK_WORKFLOW_RUNNING => .running,
            c.WK_WORKFLOW_WAITING => .waiting,
            c.WK_WORKFLOW_COMPLETED => .completed,
            c.WK_WORKFLOW_FAILED => .failed,
            else => .cancelled,
        };
    }

    /// Whether the instance has finished (completed, failed or cancelled).
    pub fn isFinished(self: State) bool {
        return self == .completed or self == .failed or self == .cancelled;
    }
};

/// An instance's status; call `deinit` when done.
pub const Status = struct {
    state: State,
    /// When waiting: until when (ms since the Unix epoch), at the latest.
    wake_at: ?u64,
    /// When completed: the workflow's output.
    output: ?[]const u8,
    /// When failed: why.
    failure: ?[]const u8,
    /// Positions recorded so far (steps, sleeps and waits).
    positions: u64,
    raw: c.wk_workflow_status,

    fn fromC(raw: c.wk_workflow_status) Status {
        return .{
            .state = State.fromC(raw.state),
            .wake_at = if (raw.wake_at == 0) null else raw.wake_at,
            .output = if (raw.has_output) fromBytes(raw.output) else null,
            .failure = if (raw.@"error".len == 0) null else fromBytes(raw.@"error"),
            .positions = raw.positions,
            .raw = raw,
        };
    }

    /// How many events it waits for.
    pub fn waitingCount(self: *const Status) usize {
        return self.raw.waiting_count;
    }

    /// The name of the `i`th event it waits for.
    pub fn waitingFor(self: *const Status, i: usize) []const u8 {
        return fromBytes(self.raw.waiting_for[i]);
    }

    pub fn deinit(self: *Status) void {
        c.wk_workflow_status_free(&self.raw);
        self.output = null;
        self.failure = null;
    }
};

/// Starts and queries instances of one workflow class: a plain value,
/// copy it freely.
pub const Client = struct {
    raw: c.wk_workflow,

    /// Instances of `class` (the workflow class's name), borrowed. Usable
    /// at comptime, so a client can be a container-level constant.
    pub fn init(class: [*:0]const u8) Client {
        return .{ .raw = .{ .class_name = class } };
    }

    /// Starts instance `id` with `input`, or returns its status if it
    /// exists (a start is idempotent while the instance is kept). It runs
    /// in its own event, soon after: this call only records it.
    pub fn start(self: *const Client, id: []const u8, input: []const u8) Error!Status {
        var raw: c.wk_workflow_status = .{};
        if (!c.wk_workflow_start(&self.raw, toBytes(id), toBytes(input), &raw, &last)) return failed();
        return Status.fromC(raw);
    }

    /// The instance's status; `error.Unknown` if there is none.
    pub fn status(self: *const Client, id: []const u8) Error!Status {
        var raw: c.wk_workflow_status = .{};
        if (!c.wk_workflow_get_status(&self.raw, toBytes(id), &raw, &last)) return failed();
        return Status.fromC(raw);
    }

    /// Delivers event `name` with `payload`: recorded durably, then the
    /// instance runs. `error.Unknown` if it was never started,
    /// `error.Finished` if it has finished.
    pub fn sendEvent(self: *const Client, id: []const u8, name: []const u8, payload: []const u8) Error!void {
        if (!c.wk_workflow_send_event(&self.raw, toBytes(id), toBytes(name), toBytes(payload), &last)) return failed();
    }

    /// Cancels the instance: it runs no further step (one running now
    /// finishes first). Returns its status; a finished one is unchanged.
    pub fn cancel(self: *const Client, id: []const u8) Error!Status {
        var raw: c.wk_workflow_status = .{};
        if (!c.wk_workflow_cancel(&self.raw, toBytes(id), &raw, &last)) return failed();
        return Status.fromC(raw);
    }
};
