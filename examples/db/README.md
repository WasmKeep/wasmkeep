# A database kept by one actor

The SDK's database pattern ([guide](../../docs/database.md),
[ADR 0029](../../docs/adr/0029-a-database-client-as-an-sdk-pattern.md)):
one actor owns an app's tables, and the app's other actors send it SQL
over `actors.call`.

| Crate | Class | Role |
| --- | --- | --- |
| [`database`](database) | `db` | one actor per database (`db/main`): its schema applied by `db::migrate`, the SDK's handler serving the app's actors only, with lowered limits so the tests reach them |
| [`app`](app) | `app` | an HTTP API over `db::Client`: notes written, listed and counted, a batch that is all or none, a write inside a read rolled back, a refused `COMMIT`, every SQL type round trip (routes at the top of [`app/src/lib.rs`](app/src/lib.rs)) |

The same two classes in TypeScript are [`examples/ts-db`](../ts-db), with
the same routes and answers; any of the four components works with
either of the other language's (`crates/wasmkeep-server/tests/db_langs.rs`
runs every pair).

## Run it

From the repository root:

```bash
(cd examples/db/database && cargo build --release --target wasm32-wasip2)
(cd examples/db/app && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor db=examples/db/database/target/wasm32-wasip2/release/db_database.wasm \
  --actor app=examples/db/app/target/wasm32-wasip2/release/db_app.wasm \
  --access db=internal
```

In another terminal:

```bash
curl -d 'buy milk' localhost:8080/actor/app/main/notes          # 1
curl -d $'eggs\nbread\n' localhost:8080/actor/app/main/notes/batch  # 2
curl localhost:8080/actor/app/main/notes                        # 1 buy milk, 2 eggs, 3 bread
curl -d $'tea\ntea\n' localhost:8080/actor/app/main/notes/batch     # sql constraint 1: nothing kept
curl localhost:8080/actor/app/main/count                        # 3
curl -i -X POST localhost:8080/actor/db/main/wasmkeep-db/v1/read   # refused: the class is internal
```

## What to copy, and what not

- **Every SQL statement is in the app's code**, its values bound as
  parameters. No route takes SQL from a request, and none should: the
  database runs whatever its callers send.
- **The database class is `internal`** and answers only the app's actors
  and front; `db::handle_allowing` narrows that to chosen callers.
- **Migrations are append-only.** Add a statement at the end of
  `MIGRATIONS`; never edit one a database has applied.
- The lowered `LIMITS` in [`database/src/lib.rs`](database/src/lib.rs) are
  for the tests; the defaults (`db::Limits::default()`) suit most apps.
