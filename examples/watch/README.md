# Watch: an uptime monitor on WasmKeep

Our dogfood application: one actor per monitored URL, checking on its own
alarm, and one hub that keeps the list, delivers alerts to a webhook with
retries, and serves a live dashboard. What building it taught us is in
[docs/dogfood.md](../../docs/dogfood.md).

| Class | Actors | Role |
| --- | --- | --- |
| [`monitor`](monitor) | one per URL (`monitor/<id>`) | GET the URL every `every_s`, keep the last 1,000 checks, go down after two failures in a row and up after one success, tell the hub, stream checks over WebSocket |
| [`hub`](hub) | one (`hub/main`) | create, update and delete monitors; record alerts durably and deliver them to the webhook with backoff; dashboard |

## Run it

From the repository root:

```bash
(cd examples/watch/monitor && cargo build --release --target wasm32-wasip2)
(cd examples/watch/hub && cargo build --release --target wasm32-wasip2)
cargo run -p wasmkeep-cli -- serve \
  --actor monitor=examples/watch/monitor/target/wasm32-wasip2/release/watch_monitor.wasm \
  --actor hub=examples/watch/hub/target/wasm32-wasip2/release/watch_hub.wasm \
  --access monitor=existing --access hub=existing:main
```

In another terminal:

```bash
H=localhost:8080/actor/hub/main
curl -X PUT $H/webhook -d 'https://hooks.slack.com/services/...'
curl -X PUT $H/monitors/site -d '{"url":"https://example.com","every_s":30}'
open http://localhost:8080/actor/hub/main/      # the dashboard
curl $H/monitors                                 # every monitor's status
curl $H/alerts                                   # alerts and their delivery
curl -X DELETE $H/monitors/site
```

Alerts are JSON with `text` and `content` fields, so Slack and Discord
incoming webhooks accept them as they are.

A check is a `GET` sent with `User-Agent: wasmkeep-watch/<version>`, and
any status below 400 counts as up. A site that answers a client it does
not know with an error (a 404, say) shows as down although browsers reach
it: allow that agent on the site, or monitor a path that answers everyone.

On a cluster, serve both classes on every node: a monitor whose node dies
keeps checking on another node.

## Configuration and access

Give the hub a token, and optionally the webhook, in a file only you can
read:

```bash
TOKEN=$(openssl rand -hex 32)
printf 'ADMIN_TOKEN=%s\nWEBHOOK_URL=https://hooks.slack.com/services/...\n' \
  "$TOKEN" > hub.env && chmod 600 hub.env
cargo run -p wasmkeep-cli -- serve --env hub=hub.env \
  --actor monitor=... --actor hub=... --access ...
```

In another terminal, with the same `TOKEN`:

```bash
H=localhost:8080/actor/hub/main
curl -X PUT $H/monitors/site -H "Authorization: Bearer $TOKEN" -d '...'
```

With `ADMIN_TOKEN` set, every change (`PUT`, `DELETE`) and `GET /webhook`
need `Authorization: Bearer <token>`; the dashboard and status reads stay
open. With `WEBHOOK_URL` set, `PUT /webhook` is refused. Monitors accept
changes only from the hub, so the token cannot be skipped by calling a
monitor directly. Without a token, anyone who can reach the hub can change
it: keep it on localhost or behind an authenticating proxy.

`--access monitor=existing` keeps outsiders from creating monitors: a
request for a monitor the hub did not create answers 404 and leaves
nothing behind. Without it, every new name requested would create an empty
monitor and its database.

`--access hub=existing:main` does the same for hubs, except that
`hub/main` itself may be created from outside (nothing else would create
it): any other `hub/<name>` answers 404.

Tested end to end in `crates/wasmkeep-server/tests/watch.rs`.
