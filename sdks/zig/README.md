# WasmKeep actors in Zig

Write [WasmKeep](https://github.com/WasmKeep/wasmkeep/blob/main/README.md)
actors as a Zig file with a few public functions. The SDK,
[`wasmkeep.zig`](https://github.com/WasmKeep/wasmkeep/blob/main/sdks/zig/wasmkeep.zig),
wraps the [C SDK](https://github.com/WasmKeep/wasmkeep/tree/main/sdks/c)
(translated with `zig translate-c`). Needs Zig 0.17. It carries the
server's version (`wk.version`) and targets the `wasmkeep:actor@0.2.0`
WIT world.

```zig
const std = @import("std");
const wk = @import("wasmkeep");

// Once per deployed version, before any other event: create tables here.
pub fn init() !void {
    _ = try wk.exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)", .{});
}

// One HTTP request at a time, in arrival order: no locks needed.
pub fn fetch(req: *wk.Request) !wk.Response {
    _ = try wk.exec("INSERT INTO hits (at) VALUES (?)", .{wk.nowMs()});
    const n = try wk.queryInt("SELECT count(*) FROM hits", .{});
    return wk.reply(200, "{d} hits on {s}\n", .{ n, req.path });
}

pub fn wsMessage(conn: u64, data: []const u8, binary: bool) void {
    _ = conn;
    if (!binary) wk.broadcast(data);
}

comptime {
    wk.actor(@This()); // exports the hooks this file declares
}
```

Starting with the release after 0.1.0, a release carries this SDK as
`wasmkeep-zig-sdk-<version>.tar.gz`, signed like its other files: the Zig
files, the C SDK with its bindings already generated, the WIT and the
componentize script, so building needs no clone and no wit-bindgen. Its
README has the build command for the extracted directory.

From a clone, build it with
[`examples/zig-counter/build.sh`](https://github.com/WasmKeep/wasmkeep/blob/main/examples/zig-counter/build.sh)
as a template (it sets up the `wasmkeep` and `wasmkeep_c` modules), then
`wasmkeep serve --actor hits=actor.wasm`. Keep its `--stack 1048576`: Zig's
default stack is 16 MiB, and it is reserved in every actor's memory, so
each resident actor would take several times the memory (2.7 MB instead
of 0.57 MB, measured with huge pages on).

## API

| | |
| --- | --- |
| Hooks | `init`, `fetch`, `alarm`, `wsOpen` (return false to refuse), `wsMessage`, `wsClose`; all optional. A returned error fails `init`/`alarm` (an alarm is retried) and makes `fetch` answer 500 |
| `wk.exec(sql, .{params})`, `wk.execDurable` | Rows changed; `error.Sql` (then `wk.lastError()`, `wk.lastErrorKind()`) |
| `wk.query(sql, .{params})` | `Rows`: `len()`, `int`, `real`, `text(row, col)`, `column("name")`, `deinit()` |
| `wk.queryInt(sql, .{params})` | First column of the first row |
| Parameters | integers, floats, bools, `null`, optionals, strings (`[]const u8`, bound as text), `wk.Blob{ .bytes = b }` |
| `wk.setAlarm(ms)`, `wk.getAlarm()`, `wk.clearAlarm()` | The actor's single alarm, ms since the Unix epoch |
| `wk.call(address, method, path, body)` | `CallResponse`: `status()`, `body()`, `deinit()`; `error.Call` |
| `wk.send`, `wk.sendBinary`, `wk.broadcast`, `wk.close`, `wk.connectionCount`, `wk.connections` | WebSockets |
| `req.method`, `req.path`, `req.body()`, `req.header("name")` | The request |
| `wk.reply(status, fmt, args)`, `wk.replyBytes(status, content_type, body)` | Responses |
| `wk.env(name)` | The class's configuration value or secret (`deploy --config`, `secrets set`, `serve --env`; last wins; see [`env`](https://github.com/WasmKeep/wasmkeep/blob/main/docs/reference/api.md#interface-env)), or `null` |
| `wk.selfAddress()` | This actor's `Address` (`tenant`, `class`, `name`) |
| `wk.setHeader(&response, name, value)` | Adds a header to a response |
| `wk.deleteSelf()`, `wk.nowMs()` | |

## Key-value and queues

`wk.kv` and `wk.queue` ([`kv.zig`](https://github.com/WasmKeep/wasmkeep/blob/main/sdks/zig/kv.zig), [`queue.zig`](https://github.com/WasmKeep/wasmkeep/blob/main/sdks/zig/queue.zig))
wrap the C SDK's clients of
[ADR 0015](https://github.com/WasmKeep/wasmkeep/blob/main/docs/adr/0015-kv-and-queues-as-sdk-patterns.md) for the
Rust shard and queue classes ([`examples/jobs`](https://github.com/WasmKeep/wasmkeep/tree/main/examples/jobs)), with
Zig errors (`wk.kv.Error`, `wk.queue.Error`; details in `lastError()`) and
slices:

```zig
const store = wk.kv.Client.init("kv", 8).withReadCache(60_000);
const jobs = wk.queue.Client.init("queue/jobs");

const version = try store.put("user:42", "Ada");
_ = try store.compareAndSet("user:42", version, "Ada L."); // error.Conflict if it moved
_ = try jobs.send("resize 42");
// In fetch: if (wk.queue.isDelivery(req)) return wk.queue.handleDelivery(req, work);
```

The guide is [Key-value and queues](https://github.com/WasmKeep/wasmkeep/blob/main/docs/kv-and-queues.md#zig); the
jobs app is [`examples/zig-jobs`](https://github.com/WasmKeep/wasmkeep/tree/main/examples/zig-jobs).

## Workflows

`wk.workflow` ([`workflow.zig`](https://github.com/WasmKeep/wasmkeep/blob/main/sdks/zig/workflow.zig)) wraps the C SDK's client
of [ADR 0019](https://github.com/WasmKeep/wasmkeep/blob/main/docs/adr/0019-workflows-as-an-sdk-pattern.md)'s
workflows, for a workflow class run by the Rust SDK
([`examples/workflows`](https://github.com/WasmKeep/wasmkeep/tree/main/examples/workflows)), with Zig errors
(`wk.workflow.Error`: `Unknown`, `Finished`, `Call`, `Refused`; details in
`lastError()`):

```zig
const orders = wk.workflow.Client.init("orders");

var status = try orders.start("order-42", "item=book"); // or .status(id), .cancel(id)
defer status.deinit(); // status.state, .wake_at, .waitingFor(i), .output, .failure
try orders.sendEvent("order-42", "approved", "");
```

The guide is [Workflows](https://github.com/WasmKeep/wasmkeep/blob/main/docs/workflows.md#zig); the jobs app's
order routes use it.

Examples: [counter](https://github.com/WasmKeep/wasmkeep/tree/main/examples/zig-counter),
[chat room](https://github.com/WasmKeep/wasmkeep/tree/main/examples/zig-chat-room),
[jobs](https://github.com/WasmKeep/wasmkeep/tree/main/examples/zig-jobs).

## License

Apache-2.0; see `LICENSE` and `NOTICE`. WasmKeep is pre-alpha.
