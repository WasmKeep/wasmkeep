# Jobs, in Go

The [jobs app](../jobs/app) in Go (TinyGo), with the
[Go module](../../sdks/go)'s `kv` and `queue` clients
([guide](../../docs/kv-and-queues.md#go)): the same routes and response
bodies as [`examples/ts-jobs`](../ts-jobs). Only the app is Go: the shard
and queue classes are the Rust components from [`examples/jobs`](../jobs)
(`kv` and `queue`), which the Go clients call over `actors.call` with the
same protocol as the Rust clients. Its `/orders` routes start, approve,
query and cancel orders of the Rust `orders` workflow class from
[`examples/workflows`](../workflows) with the module's `workflow` client
([guide](../../docs/workflows.md#go)), as the Rust app there does.

Deploy it as class `app`: the `queue/jobs` queue pushes to `app/worker`,
which answers with `queue.HandleDelivery`. The routes are listed at the top
of [`main.go`](main.go). Like [go-counter](../go-counter), it uses the
bindings `wit-bindgen-go` generates into `gen/` (ignored by git); the
`callActor` function in `main.go` adapts their `actors.Call` to the
module's `call.Caller`, and `go.mod` points at the module with a `replace`
directive.

```bash
(cd examples/go-jobs && ./build.sh)   # writes examples/go-jobs/jobs.wasm (see build.sh for the tools)
(cd examples/jobs/kv && cargo build --release --target wasm32-wasip2)
(cd examples/jobs/queue && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor kv=examples/jobs/kv/target/wasm32-wasip2/release/jobs_kv.wasm \
  --actor queue=examples/jobs/queue/target/wasm32-wasip2/release/jobs_queue.wasm \
  --actor app=examples/go-jobs/jobs.wasm \
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

For the orders, serve the workflow classes too:

```bash
(cd examples/workflows/orders && cargo build --release --target wasm32-wasip2)
(cd examples/workflows/ledger && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor orders=examples/workflows/orders/target/wasm32-wasip2/release/workflows_orders.wasm \
  --actor ledger=examples/workflows/ledger/target/wasm32-wasip2/release/workflows_ledger.wasm \
  --actor app=examples/go-jobs/jobs.wasm --access orders=internal
```

In another terminal:

```bash
A=localhost:8080/actor/app/main
curl -X POST $A/orders/o1 -d 'item=book;cooloff_ms=0;approve_within_ms=60000'
curl -X POST $A/orders/o1/approve
curl $A/orders/o1                                 # state: completed, output: shipped book
```

`crates/wasmkeep-server/tests/jobs_langs.rs` (`go`) runs it against the Rust
classes, and `crates/wasmkeep-server/tests/workflows_langs.rs` (`go`)
against the workflow classes.
