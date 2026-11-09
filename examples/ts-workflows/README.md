# Workflows in TypeScript

The order workflow of [`examples/workflows`](../workflows), written in
TypeScript with `@wasmkeep/sdk` ([guide](../../docs/workflows.md#typescript)):
reserve, cool off, wait for approval, charge, notify. Every side effect is
an HTTP request with `http.fetch` to a ledger service at `LEDGER_URL`,
carrying the step's idempotency key.

From the repository root:

```bash
(cd sdks/typescript && npm ci)
(cd examples/ts-workflows && npm ci && npm run build)  # writes examples/ts-workflows/orders.wasm (QuickJS)
(cd examples/ts-workflows && npm run build:starlingmonkey)  # or on StarlingMonkey: writes orders-starlingmonkey.wasm
```

Serve it as class `orders` next to the Rust `app` class of
`examples/workflows` (the two SDKs speak the same protocol), with the
ledger's address in its configuration (`--env orders=FILE`, a line
`LEDGER_URL=http://...`) and, for a ledger on a private address,
`--allow-outbound` for it. The ledger answers
`POST /record?what=<what>&key=<key>` with `200` and any text.

The test (`crates/wasmkeep-server/tests/workflows_ts.rs`) runs it on both
engines against a mock ledger: an approved order with a failing charge
retried, and an order whose approval never comes.
