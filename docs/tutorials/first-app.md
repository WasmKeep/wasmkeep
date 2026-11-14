# Your first app

You will run a small app with two parts: a **front**, which serves a page
and a JSON API and signs users in, and a class of actors, **`todos`**,
which keeps each user's list and which no client can reach. You will run
it on your machine with `wasmkeep dev`, then deploy it to a local bucket,
run a node, give the front its secret, change the front and roll the
change back.

This follows [Your first actor](first-actor.md). Applies to: WasmKeep
built from `main` (the release after `0.3.0`). The front, the project
file (`wasmkeep.toml`), `wasmkeep dev` without crate directories and
`secrets set --front` are all **main only**: `0.3.0` has none of them.
Commands assume Linux and one terminal: what you start runs in its
background, and the variables you set stay set.

## 1. Get the code

You need [rustup](https://rustup.rs) with the `wasm32-wasip2` target (as
in the first tutorial), `curl`, `openssl`, and
[uv](https://docs.astral.sh/uv/) or `pip` for the local bucket. Clone
WasmKeep and build the server:

<!-- test: clone -->
```bash
git clone https://github.com/wasmkeep/wasmkeep && cd wasmkeep
```

<!-- test: build-server -->
```bash
cargo build --release -p wasmkeep-cli
export PATH="$PWD/target/release:$PATH"
wasmkeep --version
```

You should see `wasmkeep` and its version.

## 2. Copy the example

The app is [`examples/todo-front`](../../examples/todo-front). Copy it, so
you can change it without touching the clone:

<!-- test: copy -->
```bash
mkdir -p .wasmkeep
cp -r examples/todo-front .wasmkeep/todo
cd .wasmkeep/todo
```

Every command below runs in `.wasmkeep/todo`. Its two crates take the SDK
from the clone (`path = "../../../crates/wasmkeep-sdk"`), which the copy
keeps at the same depth. `.wasmkeep/` is in the clone's `.gitignore`, so
the tokens, keys and logs this page writes there are never committed.

| Path | What it is |
| --- | --- |
| `front/` | the front: the page, and a JSON API of the signed-in user's list |
| `todos/` | class `todos`: one actor per user (`todos/alice`), its list in SQLite |
| `wasmkeep.toml` | the project file: the app, its parts, how each is built |

## 3. Read the project file

`wasmkeep.toml` is the whole app. `wasmkeep dev` runs what it says, and
`wasmkeep deploy` deploys exactly that
([The project file](../project-file.md)):

```toml
[app]
tenant = "acme"
name = "todo"

[front]
component = "front/target/wasm32-wasip2/release/todo_front.wasm"
build = "cd front && cargo build --release --target wasm32-wasip2"
requires = ["TOKENS"]
watch = ["front/src", "front/Cargo.toml"]

[classes.todos]
component = "todos/target/wasm32-wasip2/release/todo_todos.wasm"
build = "cd todos && cargo build --release --target wasm32-wasip2"
watch = ["todos/src", "todos/Cargo.toml"]

[dev]
port = 8080
```

(The file in the example also has comments.) Line by line:

- **`[app]`**: the app is `acme/todo`, tenant `acme`, app `todo`. Commands
  that run beside the file take it from here, so you never type `--app`
  for `dev` or `deploy`.
- **`[front]`**: the app has a front. It answers every path outside
  `/actor/` and `/_wasmkeep/`.
  - `component`: the `.wasm` file, relative to `wasmkeep.toml`.
  - `build`: the command that makes it, run in the file's directory
    before the component is read.
  - `requires = ["TOKENS"]`: the front needs the value `TOKENS`. Until it
    is set, the node does not run the front and answers 503.
  - `watch`: what `wasmkeep dev` watches; a change runs `build` again.
- **`[classes.todos]`**: one class, named `todos`, with its component,
  build and watch paths. It has no `access` line, so it is `internal`:
  only the app's front, its actors and its alarms can reach it, never a
  client.
- **`[dev]`**: `port = 8080` is where `wasmkeep dev` listens, on
  `127.0.0.1`.

There is no value and no bucket in the file: values go in `.env.local`
for `dev` and in the bucket's secrets for a deploy, and the bucket is a
flag of `deploy`. The same file deploys to any bucket.

## 4. Run it with `wasmkeep dev`

`TOKENS` is the list of users who can sign in, as `user:token` pairs,
each token at least 32 characters. Make one for `alice`, for your machine
only:

<!-- test: env-local -->
```bash
printf 'TOKENS=alice:%s\n' "$(openssl rand -hex 32)" > .env.local
chmod 600 .env.local
```

`wasmkeep dev` reads its local values from `.env.local` (also in
`.gitignore`), and warns if other users can read the file. Start it:

<!-- test: dev -->
```bash
wasmkeep dev > dev.log 2>&1 &
echo $! > dev.pid
until curl -sf -o /dev/null localhost:8080/; do sleep 1; done
```

The first start builds both crates, so it takes a while. Then `dev.log`
says what it serves:

```text
... running the project file app=acme/todo file=wasmkeep.toml
... serving the front at http://127.0.0.1:8080/
... serving todos at http://127.0.0.1:8080/actor/todos/<name>/ (access internal)
```

Open <http://localhost:8080/> in a browser and sign in with alice's token
(what follows `alice:` in `.env.local`). Or ask for the page:

<!-- test: page -->
```bash
curl -s localhost:8080/ | grep '<h1>'
```

```text
<h1>Todos</h1>
```

While `dev` runs, a saved change under a `watch` path rebuilds that part
and reloads the app; a build that fails keeps the previous one serving.

## 5. Use the API

Without a token, the API refuses you:

<!-- test: no-token -->
```bash
curl -i localhost:8080/api/todos
```

```text
HTTP/1.1 401 Unauthorized
content-type: text/plain
www-authenticate: Bearer
...

sign in with Authorization: Bearer <token>
```

With alice's token, add an item and read the list:

<!-- test: with-token -->
```bash
token=$(cut -d : -f 2 .env.local)
curl -s -w '%{http_code}\n' -H "Authorization: Bearer $token" -d 'buy milk' localhost:8080/api/todos
curl -s -w '%{http_code}\n' -H "Authorization: Bearer $token" localhost:8080/api/todos
```

```text
{"id":1}
201
[{"id":1,"text":"buy milk"}]
200
```

The front found `alice` from the token, then called her actor,
`todos/alice`, and passed its answer on. In `front/src/lib.rs`:

```rust
match actors::call(&format!("{CLASS}/{user}"), &call) {
```

`user` comes from `TOKENS`, never from the URL: each new name would be a
new actor with its own database, so the front calls only the actors of
users it signed in.

## 6. The actor is internal

Try to reach alice's actor directly:

<!-- test: actor-outside -->
```bash
curl -i localhost:8080/actor/todos/alice
```

```text
HTTP/1.1 403 Forbidden
content-type: text/plain; charset=utf-8
...

not reachable from outside
```

The node refused the request before the actor ran: `todos` is
`internal`, because its section in `wasmkeep.toml` has no `access`. The
front's calls are inside calls, from the app itself, so they reach it.
Every class of a project file is closed this way unless you write
`access = "open"` (or `existing`) for it.

The actor checks too. The first lines of its handler, in
`todos/src/lib.rs`:

```rust
if caller(&request) != Caller::Front {
    http::respond(out, 403, "text/plain", b"forbidden\n");
    return;
}
```

`caller()` reads the `x-wasmkeep-caller` header, which the host sets on
every call: `acme/_front/main` for the app's front, the calling actor's
address for an actor, and nothing for a client. A client cannot send it:
the host removes every `x-wasmkeep-` header a client sends. So if
`todos` were ever deployed `open`, a client would get this `forbidden`
from the actor instead of the node's `not reachable from outside`.

Stop `dev` (the node in step 9 uses the same port):

<!-- test: stop-dev -->
```bash
kill $(cat dev.pid)
wait $(cat dev.pid)
```

The list stays in `.wasmkeep/dev`, beside `wasmkeep.toml`, for the next
`wasmkeep dev`.

## 7. Start a bucket

As in [From one node to three](one-node-to-three.md#3-start-a-bucket),
[moto](https://github.com/getmoto/moto) runs an S3 API in memory. With
uv:

<!-- test: moto-uvx -->
```bash
uvx --from 'moto[server]==5.2.5' moto_server -p 9000 > moto.log 2>&1 &
echo $! > moto.pid
```

Or with pip, install it once (`pip install 'moto[server]==5.2.5'`), then:

<!-- test: moto-pip -->
```bash
moto_server -p 9000 > moto.log 2>&1 &
echo $! > moto.pid
```

Wait for it, create a bucket named `demo`, and point WasmKeep at it:

<!-- test: bucket -->
```bash
until curl -sf -o /dev/null localhost:9000/moto-api/; do sleep 1; done
curl -X PUT -H 'Content-Length: 0' localhost:9000/demo
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
  AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true
```

## 8. Deploy it

The front's secret will live in the bucket, sealed with a key that the
nodes hold and the bucket never sees. Make the key:

<!-- test: key -->
```bash
wasmkeep secrets new-key --out secrets.key
```

Then deploy, from the directory of `wasmkeep.toml`:

<!-- test: deploy -->
```bash
wasmkeep deploy --bucket s3://demo
```

`deploy` runs both builds, then prints what changes before it switches
(the version id differs):

```text
acme/todo from wasmkeep.toml, before switching:
  front: added
  class todos: added (internal)
  requires _front: TOKENS now required
acme/todo: version b12d4cbcc397044d is current (deploy 1)
```

The front and the class are new, `todos` is `internal` as the file says,
and the front (`_front`) requires `TOKENS`. The version holds the front
and the class together.

## 9. Start a node

A node started with `--app` serves what was last deployed for that app,
and follows each new deploy:

<!-- test: serve -->
```bash
wasmkeep serve --app acme/todo --bucket s3://demo --secrets-key secrets.key \
  --data-dir node > node.log 2>&1 &
echo $! > node.pid
until curl -sf -o /dev/null localhost:8080/_wasmkeep/ready; do sleep 1; done
```

It listens on `127.0.0.1:8080` and keeps its actors' databases in
`node/`. (A node may also start before the app's first deploy: it waits
for one, and starts serving when it lands.) Ask it for the API:

<!-- test: front-503 -->
```bash
curl -i localhost:8080/api/todos
```

```text
HTTP/1.1 503 Service Unavailable
retry-after: 1
...

this node cannot serve the app now; retry
```

The front requires `TOKENS`, and the node does not read `.env.local`:
that file is for `dev` only. So the node does not run the front, and
says why in its log, never to the client:

<!-- test: front-log -->
```bash
grep -c 'required values are not set' node.log
```

```text
1
```

## 10. Give the front its secret

Make a token for the deployed app, a new one (the values of `dev` stay on
your machine), and set it as the front's secret:

<!-- test: secret -->
```bash
(umask 077; printf 'alice:%s\n' "$(openssl rand -hex 32)" > tokens.txt)
wasmkeep secrets set --app acme/todo --bucket s3://demo --key secrets.key --front TOKENS < tokens.txt
```

```text
Set 1 secret(s) for acme/todo: seq 1.
```

`--front` sets a value of the front, as `--class NAME` sets one of a
class. A node checks for a new set of secrets every 3 s, with no deploy
and no restart. Wait for the front, then use the API again with the new
token:

<!-- test: use-node -->
```bash
until [ "$(curl -s -o /dev/null -w '%{http_code}' localhost:8080/)" = 200 ]; do sleep 1; done
token=$(cut -d : -f 2 tokens.txt)
curl -s -w '%{http_code}\n' -H "Authorization: Bearer $token" -d 'ship it' localhost:8080/api/todos
curl -s -w '%{http_code}\n' localhost:8080/actor/todos/alice
```

```text
{"id":1}
201
not reachable from outside
403
```

The id is `1` again: the node's actors are not those of `dev`. And
`todos` is as closed on the node as it was under `dev`, since the rule
is in the deployed version.

## 11. Change the front and redeploy

Change the page's heading, and deploy again:

<!-- test: change -->
```bash
sed -i 's|<h1>Todos</h1>|<h1>Our todos</h1>|' front/src/page.html
wasmkeep deploy --bucket s3://demo
```

```text
acme/todo from wasmkeep.toml, before switching:
  front: changed
acme/todo: version 22efbe932f640932 is current (deploy 2)
```

Only the front changed. The node switches to the new version within a
few seconds:

<!-- test: after-change -->
```bash
until curl -s localhost:8080/ | grep -q 'Our todos'; do sleep 1; done
curl -s localhost:8080/ | grep '<h1>'
curl -s -H "Authorization: Bearer $token" localhost:8080/api/todos
```

```text
<h1>Our todos</h1>
[{"id":1,"text":"ship it"}]
```

The list is still there: a deploy replaces code, and each actor keeps
its database. Deploy again without changing anything:

<!-- test: no-op -->
```bash
wasmkeep deploy --bucket s3://demo
```

```text
acme/todo: nothing changed: version 22efbe932f640932 is already current (deploy 2)
```

The builds run, find nothing new, and `deploy` writes nothing.

## 12. Roll back

Make the previous version current again:

<!-- test: rollback -->
```bash
wasmkeep deploy --bucket s3://demo --rollback
```

```text
  front: changed
note: wasmkeep.toml may now describe another version than the one rolled back to; the next deploy from it deploys the file again
rolled back acme/todo: 22efbe932f640932 -> b12d4cbcc397044d (deploy 3)
```

The version now current is the first deploy's again, at a new deploy
number. A
rollback runs no build: it restores the version the bucket holds, the
front with its classes, their rules and required names. The page is
back:

<!-- test: after-rollback -->
```bash
until curl -s localhost:8080/ | grep -q '<h1>Todos</h1>'; do sleep 1; done
curl -s localhost:8080/ | grep '<h1>'
```

```text
<h1>Todos</h1>
```

`front/src/page.html` still has `Our todos`, so the next `wasmkeep
deploy` deploys it again, as the note says. To keep the rollback, undo
the change in the file too.

## 13. Clean up

Stop the node and the bucket, wait for them to exit, and remove the copy:

<!-- test: cleanup -->
```bash
kill $(cat node.pid) $(cat moto.pid)
wait
cd ../..
rm -rf .wasmkeep/todo
```

moto kept the bucket in memory, so it is gone too.

## What just happened

- **The front is the app's door.** Every path outside `/actor/` reaches
  it. It signs users in, chooses which actor to call, and calls it as an
  inside caller ([The front](../concepts.md#the-front)).
- **Classes are closed by default.** A class of a project file is
  `internal` unless it says otherwise, so clients reach your actors only
  through the front. `caller()` lets an actor check it too.
- **One file, two uses.** `wasmkeep dev` and `wasmkeep deploy` read the
  same `wasmkeep.toml`, so what you try locally is what you deploy.
- **Values stay out of the file.** `.env.local` holds local values for
  `dev`; a deploy's secrets live sealed in the bucket, set with `wasmkeep
  secrets set`, and reach the nodes without a restart.
- **A version is the whole app.** The front, the classes and their rules
  deploy and roll back together; the actors' state stays.

## Next

- [From one node to three](one-node-to-three.md): run three nodes on
  your machine, kill one and watch its actors carry on.
- [The project file](../project-file.md): every key of `wasmkeep.toml`,
  and how `deploy` and `dev` read it.
- Writing a front of your own: in Rust,
  [the SDK's README](../../crates/wasmkeep-sdk/README.md#writing-a-front);
  in TypeScript, [its README](../../sdks/typescript/README.md#writing-a-front)
  and [`examples/ts-todo-front`](../../examples/ts-todo-front), the same
  front over the same `todos` class.
- [The example's README](../../examples/todo-front/README.md): what to
  copy from it (sign-in that fails closed, names checked before a call)
  and what not.
- [Secrets in the bucket](../cluster.md#secrets-in-the-bucket) and
  [Deploying the front](../cluster.md#deploying-the-front): keys, values
  and rollbacks in depth.
