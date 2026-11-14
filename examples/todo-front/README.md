# Todo front: a front over an internal class

A small app with a front ([ADR 0023](../../docs/adr/0023-the-front.md)):
the front serves a page and a JSON API, signs users in, and calls one
actor per user as an inside caller, so the actors' class can be
`internal`, out of reach of clients.

| Crate | World | Role |
| --- | --- | --- |
| [`front`](front) | `wasmkeep:actor/front` | `GET /` (the page), `GET /api`, `/api/todos` (`GET`, `POST`, `DELETE /api/todos/<id>`) for the signed-in user, and `GET /api/team`: every user's count, from one `calls::call_many` |
| [`todos`](todos) | `wasmkeep:actor/actor` | class `todos`: one actor per user (`todos/alice`), its list in SQLite (`GET /count` its size); it answers its app's front only |

The same front in TypeScript is [`examples/ts-todo-front`](../ts-todo-front),
over this `todos` class.

## Run it

The app is [`wasmkeep.toml`](wasmkeep.toml) (the
[project file](../../docs/project-file.md)): the front, the `todos`
class (`internal`, the default), how each is built, and that the front
requires `TOKENS`. From this directory:

```bash
printf 'TOKENS=alice:%s\n' "$(openssl rand -hex 32)" > .env.local
cargo run -p wasmkeep-cli -- dev
```

`wasmkeep dev` builds both crates, serves them as a deploy would, and
rebuilds and reloads when a source file changes. `.env.local` holds local
values only (it is in `.gitignore`).

Open <http://localhost:8080/> and sign in with alice's token from
`.env.local`, or with curl:

```bash
token=$(sed -n 's/^TOKENS=alice://p' .env.local)
curl -H "Authorization: Bearer $token" -d 'buy milk' localhost:8080/api/todos   # {"id":1}
curl -H "Authorization: Bearer $token" localhost:8080/api/todos                 # [{"id":1,"text":"buy milk"}]
curl -H "Authorization: Bearer $token" localhost:8080/api/team                  # {"alice":1}
curl -i localhost:8080/api/todos                                                # 401
curl -i localhost:8080/actor/todos/alice                                        # 403: internal
```

To deploy it, the same file, and `TOKENS` as a secret of the front:

```bash
wasmkeep deploy --bucket s3://my-bucket/prod      # builds, prints the plan, switches
printf 'alice:%s' "$(openssl rand -hex 32)" | \
  wasmkeep secrets set --app acme/todo --bucket s3://my-bucket/prod --key secrets.key --front TOKENS
```

Without the file, the same app is `wasmkeep serve
front/target/wasm32-wasip2/release/todo_front.wasm --actor
todos=todos/target/wasm32-wasip2/release/todo_todos.wasm --access
todos=internal --front-env front.env`, after building each crate.

## What to copy, and what not

- **Sign-in fails closed.** `TOKENS` is `user:token` pairs, comma
  separated, each token at least 32 characters. With it unset, empty or
  malformed, every API request is 401: there is no default user or token
  in the code. Tokens are compared in constant time, every entry each
  time. The page keeps the token in the tab (`sessionStorage`) and sends
  it in `Authorization`, never in a cookie, so there is no cross-site
  request to forge. A real app would issue short-lived sessions after a
  login instead of fixed tokens; keep the same rules.
- **Names are checked before any call.** The user name becomes the
  actor's name, and each new name is a new actor with its own database:
  the front only calls `todos/<user>` for a user it signed in, whose name
  is 1 to 32 of `a-z`, `0-9` and `-`. Item ids are digits only.
- **The class checks its caller too.** `todos` answers 403 unless
  `caller()` is the app's front. `--access todos=internal` already keeps
  clients out at the host; the check keeps the class closed if it is ever
  deployed `open`. A client cannot pose as the front: the host removes any
  `x-wasmkeep-` header a client sends, and sets `x-wasmkeep-caller`
  itself on every call.
- **Failed calls answer 503, without the detail.** `front::unavailable`
  turns `failed` and `timeout` into `503` with `Retry-After: 1`; the
  error itself goes to the log (`eprintln!`), not to the client.
- **Errors stay in the log.** The actor answers a storage error with a
  generic 500 and logs the detail, since the front passes its body on.
- **`scheme()` and `host()` are for links.** `GET /api` builds its URL
  from them, and escapes it: the client chooses `Host`. `X-Forwarded-Proto` comes from whoever sent the request:
  behind a proxy that sets it ([the front door](../../docs/deploy/front-door.md))
  it is the client's scheme, without one a client chooses it. Never base
  a security decision on it.
