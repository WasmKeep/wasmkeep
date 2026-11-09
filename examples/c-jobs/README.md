# Jobs, in C

The [jobs app](../jobs/app) in C, with the [C SDK](../../sdks/c)'s `kv`
and `queue` clients ([guide](../../docs/kv-and-queues.md#c)). Only the app
is C: the shard and queue classes are the Rust components from
[`examples/jobs`](../jobs) (`kv` and `queue`), which the C SDK calls over
`actors.call` with the same protocol as the Rust clients.

Deploy it as class `app`: the `queue/jobs` queue pushes to `app/worker`,
which answers with `wk_queue_handle_delivery`. The routes and bodies are
the [TypeScript app](../ts-jobs)'s (listed at the top of
[`jobs.c`](jobs.c)).

```bash
(cd examples/c-jobs && ./build.sh)   # writes examples/c-jobs/jobs.wasm (see build.sh for the tools)
(cd examples/jobs/kv && cargo build --release --target wasm32-wasip2)
(cd examples/jobs/queue && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor kv=examples/jobs/kv/target/wasm32-wasip2/release/jobs_kv.wasm \
  --actor queue=examples/jobs/queue/target/wasm32-wasip2/release/jobs_queue.wasm \
  --actor app=examples/c-jobs/jobs.wasm \
  --access kv=internal --access queue=internal
```

In another terminal:

```bash
A=localhost:8080/actor/app/main
curl -X PUT $A/kv/greeting -d hello -i            # etag: the version
curl -X PUT $A/kv/greeting -H 'if-match: 1' -d hi # 409 if the version moved
curl "$A/kv?prefix=g"
curl -X POST $A/jobs -d 'resize 42'               # the job id
curl $A/kv/job:1                                  # RESIZE 42, once the worker ran
curl -X POST $A/inbox -d hello
curl -X POST "$A/inbox/lease?visibility_ms=10000" # <id> <attempt> <body>
curl -X POST "$A/inbox/ack?id=1&attempt=1"
```

It also has the order routes of the [workflows app](../workflows/app),
over the C SDK's workflow client
([guide](../../docs/workflows.md#c)): they start, query, approve and
cancel orders of the Rust `orders` workflow class. To try them, serve
[`examples/workflows`](../workflows)' `orders` and `ledger` classes too:

```bash
(cd examples/workflows/orders && cargo build --release --target wasm32-wasip2)
(cd examples/workflows/ledger && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor orders=examples/workflows/orders/target/wasm32-wasip2/release/workflows_orders.wasm \
  --actor ledger=examples/workflows/ledger/target/wasm32-wasip2/release/workflows_ledger.wasm \
  --actor app=examples/c-jobs/jobs.wasm \
  --access orders=internal
```

In another terminal:

```bash
curl -X POST localhost:8080/actor/app/main/orders/42 -d 'item=book;cooloff_ms=0'
curl localhost:8080/actor/app/main/orders/42          # state: waiting, waiting_for: approved
curl -X POST localhost:8080/actor/app/main/orders/42/approve
curl localhost:8080/actor/app/main/orders/42          # state: completed, output: shipped book
```

`crates/wasmkeep-server/tests/jobs_langs.rs` runs it against the Rust classes,
and `crates/wasmkeep-server/tests/workflows_langs.rs` drives an order through it.
