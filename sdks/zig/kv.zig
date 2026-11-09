//! Key-value storage built from actors (ADR 0015): the client side, over
//! the C SDK's (`wasmkeep_kv.h`). Keys are spread over `shards` actors of one
//! class (`<class>/shard-0` ..), chosen by FNV-1a 64 of the key; the shard
//! class is the Rust SDK's (examples/jobs/kv).
//!
//! ```zig
//! const store = wk.kv.Client.init("kv", 8);
//! const v1 = try store.put("user:42", "Ada");
//! if (try store.get("user:42")) |found| {
//!     var entry = found;
//!     defer entry.deinit();
//!     // entry.value, entry.version
//! }
//! _ = try store.compareAndSet("user:42", v1, "Ada L."); // error.Conflict if it moved
//! ```
const std = @import("std");
const c = @import("wasmkeep_c");

fn toBytes(s: []const u8) c.wk_bytes {
    return .{ .ptr = s.ptr, .len = s.len };
}

fn fromBytes(b: c.wk_bytes) []const u8 {
    return if (b.len == 0) "" else b.ptr[0..b.len];
}

/// Keys are at most this long (bytes).
pub const max_key_len: usize = c.WK_KV_MAX_KEY_LEN;

/// Why an operation failed; `lastError()` and `lastStatus()` say more.
/// - `Conflict`: a conditional write found another version (or a key
///   where it expected none);
/// - `KeyTooLong`: the key is longer than `max_key_len`;
/// - `Storage`: the shard's database refused the operation;
/// - `Call`: the call to the shard failed; a write may have applied;
/// - `Refused`: the shard answered with an unexpected status.
pub const Error = error{ Conflict, KeyTooLong, Storage, Call, Refused };

var last: c.wk_kv_error = .{};

/// The last failure's message, worded as in the Rust SDK (valid until
/// the next failure).
pub fn lastError() []const u8 {
    return std.mem.sliceTo(&last.message, 0);
}

/// The shard's status for the last `error.Refused` (200: a malformed
/// reply).
pub fn lastStatus() u16 {
    return last.status;
}

fn failed() Error {
    return switch (last.kind) {
        c.WK_KV_CONFLICT => error.Conflict,
        c.WK_KV_KEY_TOO_LONG => error.KeyTooLong,
        c.WK_KV_STORAGE => error.Storage,
        c.WK_KV_CALL => error.Call,
        else => error.Refused,
    };
}

/// What a conditional write expects to find.
pub const Expect = union(enum) {
    any,
    absent,
    version: u64,

    fn toC(self: Expect) c.wk_kv_expect {
        return switch (self) {
            .any => c.wk_kv_expect_any(),
            .absent => c.wk_kv_expect_absent(),
            .version => |v| c.wk_kv_expect_version(v),
        };
    }
};

/// Options for `Client.putWith`.
pub const PutOptions = struct {
    expect: Expect = .any,
    /// Expire the key this many milliseconds after the write.
    ttl_ms: ?u64 = null,
    /// Return only once the write is durable; false acknowledges once
    /// committed on the shard's node.
    durable: bool = true,
};

/// A stored value; call `deinit` when done.
pub const Entry = struct {
    value: []const u8,
    /// Changes on every write, never reused within a shard.
    version: u64,
    /// When it expires (ms since the Unix epoch), if it does.
    expires_at: ?u64,
    raw: c.wk_kv_entry,

    fn fromC(raw: c.wk_kv_entry) Entry {
        return .{
            .value = fromBytes(raw.value),
            .version = raw.version,
            .expires_at = if (raw.expires_at == 0) null else raw.expires_at,
            .raw = raw,
        };
    }

    pub fn deinit(self: *Entry) void {
        c.wk_kv_entry_free(&self.raw);
        self.value = "";
    }
};

/// Keys from `Client.list`; call `deinit` when done.
pub const Keys = struct {
    raw: c.wk_kv_keys,

    pub fn len(self: *const Keys) usize {
        return self.raw.count;
    }

    pub fn at(self: *const Keys, i: usize) []const u8 {
        return fromBytes(self.raw.keys[i]);
    }

    pub fn deinit(self: *Keys) void {
        c.wk_kv_keys_free(&self.raw);
    }
};

/// The shard (0-based) that holds `key` among `shards` (zero counts as
/// one); matches the Rust SDK.
pub fn shardFor(key: []const u8, shards: u32) u32 {
    return c.wk_kv_shard_for(toBytes(key), shards);
}

/// Reaches the shards of one class: a plain value, copy it freely.
pub const Client = struct {
    raw: c.wk_kv,

    /// Keys over `shards` actors of `class` (at least one); every
    /// caller must use the same count. `class` is borrowed. Usable at
    /// comptime, so a client can be a container-level constant.
    pub fn init(class: [*:0]const u8, shards: u32) Client {
        return .{ .raw = .{ .class_name = class, .shards = @max(shards, 1), .cache_ms = 0 } };
    }

    /// Serve `get` from this instance's cache for up to `max_age_ms`;
    /// this actor's own writes through the client update it.
    pub fn withReadCache(self: Client, max_age_ms: u64) Client {
        var out = self;
        out.raw.cache_ms = max_age_ms;
        return out;
    }

    fn found(result: c_int, raw: c.wk_kv_entry) Error!?Entry {
        if (result < 0) return failed();
        if (result == 0) return null;
        return Entry.fromC(raw);
    }

    /// The entry at `key` (null if absent), from the read cache if one
    /// is set and fresh.
    pub fn get(self: *const Client, key: []const u8) Error!?Entry {
        var raw: c.wk_kv_entry = .{};
        return found(c.wk_kv_get(&self.raw, toBytes(key), &raw, &last), raw);
    }

    /// The entry at `key`, always asking the shard.
    pub fn getFresh(self: *const Client, key: []const u8) Error!?Entry {
        var raw: c.wk_kv_entry = .{};
        return found(c.wk_kv_get_fresh(&self.raw, toBytes(key), &raw, &last), raw);
    }

    /// Stores `value` at `key` (durably); returns the new version.
    pub fn put(self: *const Client, key: []const u8, value: []const u8) Error!u64 {
        return self.putWith(key, value, .{});
    }

    /// Stores `value` with `options`; returns the new version.
    pub fn putWith(self: *const Client, key: []const u8, value: []const u8, options: PutOptions) Error!u64 {
        const raw: c.wk_kv_put_options = .{
            .expect = options.expect.toC(),
            .has_ttl = options.ttl_ms != null,
            .ttl_ms = options.ttl_ms orelse 0,
            .local = !options.durable,
        };
        var version: u64 = 0;
        if (!c.wk_kv_put_with(&self.raw, toBytes(key), toBytes(value), &raw, &version, &last)) return failed();
        return version;
    }

    /// Stores `value` only if `key` is at `expected` (null: absent);
    /// `error.Conflict` otherwise.
    pub fn compareAndSet(self: *const Client, key: []const u8, expected: ?u64, value: []const u8) Error!u64 {
        const expect: Expect = if (expected) |v| .{ .version = v } else .absent;
        return self.putWith(key, value, .{ .expect = expect });
    }

    /// Removes `key`; true if it was there.
    pub fn delete(self: *const Client, key: []const u8) Error!bool {
        var removed = false;
        if (!c.wk_kv_delete(&self.raw, toBytes(key), &removed, &last)) return failed();
        return removed;
    }

    /// Removes `key` only if it holds `version`; `error.Conflict`
    /// otherwise.
    pub fn deleteIf(self: *const Client, key: []const u8, version: u64) Error!bool {
        var removed = false;
        if (!c.wk_kv_delete_if(&self.raw, toBytes(key), version, &removed, &last)) return failed();
        return removed;
    }

    /// Up to `limit` keys starting with `prefix`, in byte order, after
    /// `after` (exclusive) if given. Asks every shard.
    pub fn list(self: *const Client, prefix: []const u8, after: ?[]const u8, limit: u32) Error!Keys {
        var keys: Keys = .{ .raw = .{} };
        const a = toBytes(after orelse "");
        const after_ptr: [*c]const c.wk_bytes = if (after != null) &a else null;
        if (!c.wk_kv_list(&self.raw, toBytes(prefix), after_ptr, limit, &keys.raw, &last)) return failed();
        return keys;
    }
};
