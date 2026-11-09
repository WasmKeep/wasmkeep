# WasmKeep actors in C

Write [WasmKeep](../../README.md) actors as a few plain C functions. The SDK
is one header and its source ([`wasmkeep.h`](wasmkeep.h),
[`wasmkeep.c`](wasmkeep.c)) over the bindings `wit-bindgen c` generates,
plus the key-value and queue clients ([`wasmkeep_kv.h`](wasmkeep_kv.h),
included by `wasmkeep.h`; [`wasmkeep_kv.c`](wasmkeep_kv.c),
[`wasmkeep_wire.c`](wasmkeep_wire.c)) and the workflow client
([`wasmkeep_workflow.h`](wasmkeep_workflow.h), also included;
[`wasmkeep_workflow.c`](wasmkeep_workflow.c)); it works from C++ too.

```c
#include "wasmkeep.h"

/* Once per deployed version, before any other event: create tables here. */
void wk_init(void) {
  wk_exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)");
}

/* One HTTP request at a time, in arrival order: no locks needed. */
wk_response wk_fetch(wk_request *req) {
  wk_exec("INSERT INTO hits (at) VALUES (?)", wk_int(wk_now_ms()));
  long long n = wk_query_int("SELECT count(*) FROM hits");
  return wk_reply(200, "%lld hits on %s\n", n, req->path);
}

/* WebSockets: define only the hooks you need. */
void wk_ws_message(uint64_t conn, wk_bytes data, bool binary) {
  (void)conn;
  if (!binary) wk_broadcast_n(data.ptr, data.len);
}
```

Build it (clang with wasi-libc, or wasi-sdk) and serve it; see
[`examples/c-counter/build.sh`](../../examples/c-counter/build.sh), which
this follows:

```bash
wit-bindgen c crates/wasmkeep-wit/wit --world actor --out-dir gen
# wasi-libc: /usr on Debian, /usr/share/wasi-sysroot on Arch,
# $WASI_SDK/share/wasi-sysroot with wasi-sdk (also set CC=$WASI_SDK/bin/clang)
SYSROOT=${SYSROOT:-$([ -d /usr/share/wasi-sysroot ] && echo /usr/share/wasi-sysroot || echo /usr)}
${CC:-clang} --target=wasm32-wasi --sysroot="$SYSROOT" -O2 -mexec-model=reactor -Igen -Isdks/c \
  app.c sdks/c/wasmkeep.c sdks/c/wasmkeep_kv.c sdks/c/wasmkeep_wire.c sdks/c/wasmkeep_workflow.c \
  gen/actor.c gen/actor_component_type.o -o core.wasm
scripts/componentize-core.sh core.wasm actor.wasm
wasmkeep serve --actor hits=actor.wasm
```

## API

| | |
| --- | --- |
| Hooks | `wk_init`, `wk_fetch`, `wk_alarm`, `wk_ws_open` (return false to refuse), `wk_ws_message`, `wk_ws_close`; each has a default |
| Parameters | `wk_null()`, `wk_int(i)`, `wk_real(d)`, `wk_text(s)`, `wk_text_n(s, n)`, `wk_blob(p, n)`, passed after the SQL |
| `wk_exec(sql, ...)`, `wk_exec_durable(sql, ...)` | Rows changed, or -1 (then `wk_error()`, `wk_error_kind()`) |
| `wk_query(&rows, sql, ...)` | `rows.count`, `wk_get_int` / `wk_get_text` / `wk_get(&rows, row, col)`, `wk_column(&rows, "name")`, then `wk_rows_free` |
| `wk_query_int(sql, ...)` | First column of the first row |
| `wk_alarm_set(ms)`, `wk_alarm_get(&ms)`, `wk_alarm_clear()` | The actor's single alarm, ms since the Unix epoch |
| `wk_call(address, method, path, body, len, &resp)` | Calls another actor; `resp.status`, `resp.body`, then `wk_call_response_free` |
| `wk_call_with_headers(address, method, path, headers, count, body, len, &resp)` | The same with request headers (`wk_header` name/value pairs) |
| `wk_send`, `wk_send_n`, `wk_send_binary`, `wk_broadcast`, `wk_broadcast_n`, `wk_close`, `wk_connections` | WebSockets |
| `wk_request_body(req)`, `wk_request_header(req, "name")` | The request (`req->method`, `req->path`) |
| `wk_reply(status, fmt, ...)`, `wk_reply_copy`, `wk_reply_static` | Responses |
| `wk_response_header(&r, name, value)` | Adds a header to a response (copied) |
| `wk_fail(msg)` | Fails the current `init` or `alarm` event (an alarm is retried) |
| `wk_env(name)` | The class's configuration value or secret (`deploy --config`, `secrets set`, `serve --env`; last wins; see [`env`](../../docs/reference/api.md#interface-env)), or `NULL`; kept for the instance's life |
| `wk_self_address()` | This actor's `wk_address` (`tenant`, `class_name`, `name`; NUL-terminated, kept for the instance's life) |
| `wk_delete_self()`, `wk_now_ms()` | |

Text and blobs read back are `wk_bytes` (pointer and length, not
NUL-terminated; print with `"%.*s", (int)b.len, b.ptr`), valid until the rows
or response holding them are freed. The parameter macros use the GNU `##`
extension (clang and GCC); `wk_exec_v` and friends take an array instead.

## Key-value and queues

The clients of [ADR 0015](../../docs/adr/0015-kv-and-queues-as-sdk-patterns.md)
for the Rust shard and queue classes ([`examples/jobs`](../../examples/jobs)):
`wk_kv` (`wk_kv_get`, `wk_kv_get_fresh`, `wk_kv_put`, `wk_kv_put_with`,
`wk_kv_compare_and_set`, `wk_kv_delete`, `wk_kv_delete_if`, `wk_kv_list`,
`wk_kv_with_read_cache`), `wk_queue` (`wk_queue_send`,
`wk_queue_send_delayed`, `wk_queue_send_batch`, `wk_queue_lease`,
`wk_queue_ack`, `wk_queue_retry`, `wk_queue_settle`, `wk_queue_get_stats`,
`wk_queue_get_dead_letters`) and `wk_queue_handle_delivery` for push
consumers. Calls return `false` (`wk_kv_get`: `1` found, `0` absent, `-1`)
and fill an optional `wk_kv_error` / `wk_queue_error`; what they return
with memory has a `_free` function.

```c
static const wk_kv store = {.class_name = "kv", .shards = 8};
wk_kv_error err;
uint64_t version;
if (!wk_kv_put(&store, wk_str("user:42"), wk_str("Ada"), &version, &err))
  return wk_reply(502, "%s\n", err.message);
```

The guide is [Key-value and queues](../../docs/kv-and-queues.md#c); the
jobs app is [`examples/c-jobs`](../../examples/c-jobs). `test/run.sh` runs
the native unit tests of the framing, codecs, shard hash, read cache,
workflow status and clients with the host's C compiler.

## Workflows

The client side of [ADR 0019](../../docs/adr/0019-workflows-as-an-sdk-pattern.md)'s
workflows, for a workflow class run by the Rust SDK
([`examples/workflows`](../../examples/workflows)): `wk_workflow`
(`wk_workflow_start`, `wk_workflow_get_status`, `wk_workflow_send_event`,
`wk_workflow_cancel`). Calls return `false` and fill an optional
`wk_workflow_error` (`WK_WORKFLOW_UNKNOWN`, `WK_WORKFLOW_FINISHED`,
`WK_WORKFLOW_CALL`, `WK_WORKFLOW_REFUSED`); a `wk_workflow_status` is freed
with `wk_workflow_status_free`.

```c
static const wk_workflow orders = {.class_name = "orders"};
wk_workflow_status s;
wk_workflow_error err;
if (!wk_workflow_start(&orders, wk_str("order-42"), wk_str("item=book"), &s, &err))
  return wk_reply(502, "%s\n", err.message);
wk_response r = wk_reply(200, "%s\n", wk_workflow_state_name(s.state));
wk_workflow_status_free(&s);
```

The guide is [Workflows](../../docs/workflows.md#c); the jobs app's
order routes use it.

Examples: [counter](../../examples/c-counter), [chat room](../../examples/c-chat-room), [jobs](../../examples/c-jobs).
For Zig, see the [Zig SDK](../zig), built on this one.
