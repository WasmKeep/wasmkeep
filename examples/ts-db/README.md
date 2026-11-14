# A database kept by one actor, in TypeScript

The [db example](../db) in TypeScript, with the
[`@wasmkeep/sdk`](../../sdks/typescript) `db` handler and client
([guide](../../docs/database.md)): [`src/database.ts`](src/database.ts) is
the database class (`db.define` with its migrations and limits) and
[`src/app.ts`](src/app.ts) the app, with the Rust app's routes and the
same answers. Either works with the Rust class of the other kind: the
wire format is the same byte for byte.

From the repository root:

```bash
(cd sdks/typescript && npm ci)
(cd examples/ts-db && npm ci && npm run build)  # writes database.wasm and app.wasm
(cd examples/ts-db && npm run build:starlingmonkey)  # or on StarlingMonkey: *-starlingmonkey.wasm
cargo run -p wasmkeep-cli -- serve \
  --actor db=examples/ts-db/database.wasm \
  --actor app=examples/ts-db/app.wasm \
  --access db=internal
```

Then the requests in the [Rust example's README](../db/README.md#run-it).
