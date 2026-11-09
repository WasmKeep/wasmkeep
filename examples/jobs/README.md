# Jobs: key-value and queues as actors

The Rust SDK's KV and queue patterns ([ADR 0015](../../docs/adr/0015-kv-and-queues-as-sdk-patterns.md),
[guide](../../docs/kv-and-queues.md)) in one small app. Nothing here is a
platform feature: the shards and the queues are ordinary actors with an
SQLite table, reached with `actors.call`.

| Class | Actors | Role |
| --- | --- | --- |
| [`kv`](kv) | `kv/shard-0` .. `kv/shard-3` | the SDK's ready-made shard (`wasmkeep_sdk::kv::Shard`): keys, versions, expiry |
| [`queue`](queue) | `queue/jobs`, `queue/inbox` | one queue each: `jobs` pushes to `app/worker` from its alarm, `inbox` is pull-only |
| [`app`](app) | any (`app/main`), and `app/worker` | the HTTP API below; `app/worker` is the consumer `jobs` pushes to |

## Run it

From the repository root:

```bash
(cd examples/jobs/kv && cargo build --release --target wasm32-wasip2)
(cd examples/jobs/queue && cargo build --release --target wasm32-wasip2)
(cd examples/jobs/app && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor kv=examples/jobs/kv/target/wasm32-wasip2/release/jobs_kv.wasm \
  --actor queue=examples/jobs/queue/target/wasm32-wasip2/release/jobs_queue.wasm \
  --actor app=examples/jobs/app/target/wasm32-wasip2/release/jobs_app.wasm \
  --access kv=internal --access queue=internal
```

`--access ... internal` keeps outside clients away from the shards and
queues before an actor is even created; the SDK also refuses them (`403`)
on its own, for a server started without the rule.

In another terminal:

```bash
A=localhost:8080/actor/app/main
curl -X PUT $A/kv/greeting -d hello -i            # etag: the version
curl $A/kv/greeting
curl -X PUT $A/kv/greeting -H 'if-match: 1' -d hi # 409 if the version moved
curl -X PUT "$A/kv/session?ttl_ms=5000" -d s      # gone after 5 s
curl "$A/kv?prefix=g"

curl -X POST $A/jobs -d 'resize 42'               # the job id
curl $A/kv/job:1                                  # RESIZE 42, once the worker ran
curl -X POST $A/jobs -d 'fail always'             # retried, then dead-lettered
curl $A/jobs/stats
curl $A/jobs/dead

curl -X POST $A/inbox -d hello                    # a pull queue
curl -X POST "$A/inbox/lease?visibility_ms=10000" # <id> <attempt> <body>
curl -X POST "$A/inbox/ack?id=1&attempt=1"
```

The routes are listed at the top of [`app/src/lib.rs`](app/src/lib.rs). The
tests in `crates/wasmkeep-server/tests/jobs.rs` run every one of them.

The same app in TypeScript, Python, Go, C and Zig, against these `kv` and
`queue` classes: [`examples/ts-jobs`](../ts-jobs),
[`examples/py-jobs`](../py-jobs), [`examples/go-jobs`](../go-jobs),
[`examples/c-jobs`](../c-jobs), [`examples/zig-jobs`](../zig-jobs).

## What to notice

- **A version per write.** `PUT` answers the key's new version; `if-match`
  makes the write conditional. Versions are never reused within a shard,
  not even after a delete, so a stale `if-match` always loses.
- **Writes are durable when they return** (`exec-durable`). A KV read from
  `app/main` asks the shard each time; `?cached=1` reads through the
  caller's 60 s cache instead, which the caller's own writes keep current.
- **At least once.** The worker may see a job twice (a lost reply, a
  failover); writing `job:<id>` is idempotent, so that is harmless. A
  consumer whose work is not idempotent records the message ids it handled.
- **One actor, one event at a time.** Each shard and each queue handles one
  operation at a time: spread hot data over more shards or queues.
