# Todo front, in TypeScript

The JSON API of the [Rust todo front](../todo-front) as a TypeScript
front (`@wasmkeep/sdk/front`, built with `--world front`), over the same
`todos` class, built from [`examples/todo-front/todos`](../todo-front/todos).
Sign-in, name checks and error answers follow the same rules; see
[what to copy](../todo-front/README.md#what-to-copy-and-what-not).

From the repository root:

```bash
(cd sdks/typescript && npm ci)
(cd examples/ts-todo-front && npm ci && npm run build)  # writes examples/ts-todo-front/front.wasm
(cd examples/ts-todo-front && npm run build:starlingmonkey)  # or on StarlingMonkey: front-starlingmonkey.wasm
(cd examples/todo-front/todos && cargo build --release --target wasm32-wasip2)
printf 'TOKENS=alice:%s\n' "$(openssl rand -hex 32)" > front.env
cargo run -p wasmkeep-cli -- serve examples/ts-todo-front/front.wasm \
  --actor todos=examples/todo-front/todos/target/wasm32-wasip2/release/todo_todos.wasm \
  --access todos=internal --front-env front.env
```

```bash
token=$(sed -n 's/^TOKENS=alice://p' front.env)
curl -H "Authorization: Bearer $token" -d 'buy milk' localhost:8080/api/todos   # {"id":1}
curl -H "Authorization: Bearer $token" localhost:8080/api/todos                 # [{"id":1,"text":"buy milk"}]
curl -H "Authorization: Bearer $token" localhost:8080/api/team                  # {"alice":1}: actors.callMany
```
