# Jobs, in TypeScript

The [jobs app](../jobs/app) in TypeScript, with the
[`@wasmkeep/sdk`](../../sdks/typescript) `kv` and `queue` clients
([guide](../../docs/kv-and-queues.md#typescript)). Only the app is
TypeScript: the shard and queue classes are the Rust components from
[`examples/jobs`](../jobs) (`kv` and `queue`), which the TypeScript SDK
calls over `actors.call` with the same protocol as the Rust clients.

Deploy it as class `app`: the `queue/jobs` queue pushes to `app/worker`,
which answers with `queue.handleDelivery`. The routes are the Rust app's
(listed at the top of [`src/index.ts`](src/index.ts)), plus a conditional
delete (`DELETE` with `if-match`).

From the repository root:

```bash
(cd sdks/typescript && npm ci)
(cd examples/ts-jobs && npm ci && npm run build)  # writes examples/ts-jobs/jobs.wasm
(cd examples/ts-jobs && npm run build:starlingmonkey)  # or on StarlingMonkey: writes jobs-starlingmonkey.wasm
(cd examples/jobs/kv && cargo build --release --target wasm32-wasip2)
(cd examples/jobs/queue && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor kv=examples/jobs/kv/target/wasm32-wasip2/release/jobs_kv.wasm \
  --actor queue=examples/jobs/queue/target/wasm32-wasip2/release/jobs_queue.wasm \
  --actor app=examples/ts-jobs/jobs.wasm \
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

`crates/wasmkeep-server/tests/jobs_langs.rs` runs it against the Rust classes,
on both engines.
