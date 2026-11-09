# Jobs, in Python

The [jobs app](../jobs/app) in Python, with the [Python SDK](../../sdks/python)'s
`kv` and `queue` clients ([guide](../../docs/kv-and-queues.md#python)). Only
the app is Python: the shard and queue classes are the Rust components from
[`examples/jobs`](../jobs) (`kv` and `queue`), which the Python SDK calls
over `actors.call` with the same protocol as the Rust clients. The component
embeds CPython (the counter's is about 19 MB).

Deploy it as class `app`: the `queue/jobs` queue pushes to `app/worker`,
which answers with `queue.handle_delivery`. The routes are the
[TypeScript app](../ts-jobs)'s (listed at the top of [`app.py`](app.py)).

```bash
pip install componentize-py==0.25.1
(cd examples/py-jobs && ./build.sh)   # writes examples/py-jobs/jobs.wasm
(cd examples/jobs/kv && cargo build --release --target wasm32-wasip2)
(cd examples/jobs/queue && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor kv=examples/jobs/kv/target/wasm32-wasip2/release/jobs_kv.wasm \
  --actor queue=examples/jobs/queue/target/wasm32-wasip2/release/jobs_queue.wasm \
  --actor app=examples/py-jobs/jobs.wasm \
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

It also starts and follows orders of the Rust `orders` workflow class
([`examples/workflows`](../workflows)) with the SDK's `workflow` client
([guide](../../docs/workflows.md#python)), with the routes of
[the Rust app](../workflows/app): `POST /orders/<id>` (the body is the
input), `GET /orders/<id>`, `POST /orders/<id>/approve` and
`POST /orders/<id>/cancel`, answering the status as `key: value` lines.
Serve it with the workflow classes instead of (or besides) the jobs ones:

```bash
(cd examples/workflows/orders && cargo build --release --target wasm32-wasip2)
(cd examples/workflows/ledger && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor orders=examples/workflows/orders/target/wasm32-wasip2/release/workflows_orders.wasm \
  --actor ledger=examples/workflows/ledger/target/wasm32-wasip2/release/workflows_ledger.wasm \
  --actor app=examples/py-jobs/jobs.wasm \
  --access orders=internal
```

In another terminal:

```bash
A=localhost:8080/actor/app/main
curl -X POST $A/orders/o1 -d 'item=book;cooloff_ms=0'
curl $A/orders/o1                                 # waiting_for: approved
curl -X POST $A/orders/o1/approve
curl $A/orders/o1                                 # output: shipped book
```

`crates/wasmkeep-server/tests/jobs_langs.rs` and `workflows_langs.rs`
(`python`) run it against the Rust classes.
