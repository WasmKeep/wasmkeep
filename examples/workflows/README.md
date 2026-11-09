# Workflows: an order as a durable workflow

The Rust SDK's workflow runner ([ADR 0019](../../docs/adr/0019-workflows-as-an-sdk-pattern.md),
[guide](../../docs/workflows.md)) in one small app. Nothing here is a
platform feature: each order is an ordinary actor with an SQLite table,
moved forward by its alarm.

| Class | Actors | Role |
| --- | --- | --- |
| [`orders`](orders) | one per order (`orders/o1`) | the workflow: reserve, cool off, wait for approval, charge, notify |
| [`ledger`](ledger) | `ledger/main` | stands in for stock, payments and mail: records every call with its idempotency key, and can fail on purpose |
| [`app`](app) | `app/main` | the HTTP API below |

## Run it

From the repository root:

```bash
(cd examples/workflows/orders && cargo build --release --target wasm32-wasip2)
(cd examples/workflows/ledger && cargo build --release --target wasm32-wasip2)
(cd examples/workflows/app && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor orders=examples/workflows/orders/target/wasm32-wasip2/release/workflows_orders.wasm \
  --actor ledger=examples/workflows/ledger/target/wasm32-wasip2/release/workflows_ledger.wasm \
  --actor app=examples/workflows/app/target/wasm32-wasip2/release/workflows_app.wasm \
  --access orders=internal
```

In another terminal:

```bash
A=localhost:8080/actor/app/main
curl -X POST $A/orders/o1 -d 'item=book;cooloff_ms=2000;approve_within_ms=60000'
curl $A/orders/o1                         # waiting, then waiting_for: approved
curl -X POST $A/orders/o1/approve
curl $A/orders/o1                         # completed, output: shipped book
curl localhost:8080/actor/ledger/main/entries
```

Each ledger line is `what key n`: `n` above 1 would mean a step ran twice
with the same key. Try a failing step:

```bash
curl -X POST 'localhost:8080/actor/ledger/main/fail?what=charge&times=2'
curl -X POST $A/orders/o2 -d 'item=pen;cooloff_ms=0'
curl -X POST $A/orders/o2/approve         # charge fails twice, then works
```

The order's timings come from `--env orders=FILE`: `RETENTION_MS` (how
long a finished order is kept, default 7 days), `BACKOFF_MS` (first retry
delay, default 1 s; it doubles up to 30 times that), `ATTEMPTS` (default 5)
and `RUN_BUDGET_MS` (time a run spends on new steps before continuing in a
new event, default 20 s). The tests
(`crates/wasmkeep-server/tests/workflows.rs`) run every case: retries, a
step that runs out of attempts, an approval that never comes, cancel,
unknown ids, retention, and a step that traps.
