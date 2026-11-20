# Reconnecting room, in TypeScript

A public lobby built from the SDK's directory, room, pusher, edge and relay
classes ([ADR 0040](../../docs/adr/0040-directory-broadcast-groups-and-the-reconnecting-room.md)).
The room numbers messages and keeps the history; the pusher pulls from it and
delivers to the edges, so a slow delivery round never holds a publish.
The front serves a plain HTML page and bundles `@wasmkeep/sdk/room-client`:
messages, send box, connection state, delivery counters and a button that drops
the socket. Rejoining always goes through the front and directory; missing
messages come back over HTTP. No framework or external browser assets.

From the repository root:

```bash
(cd sdks/typescript && npm ci)
(cd examples/ts-big-room && npm ci && npm run build)
cargo run -j 4 -p wasmkeep-cli -- serve examples/ts-big-room/front.wasm \
  --actor room=examples/ts-big-room/room.wasm \
  --actor pusher=examples/ts-big-room/pusher.wasm \
  --actor edge=examples/ts-big-room/edge.wasm \
  --actor relay=examples/ts-big-room/relay.wasm \
  --actor directory=examples/ts-big-room/directory.wasm \
  --access room=internal --access pusher=internal --access edge=internal \
  --access relay=internal --access directory=internal
```

Open <http://localhost:8080> in two tabs and choose different names. Send a
message, drop a connection and send another while it reconnects. The message
list uses text nodes, including for names and message bodies. A failed send
keeps its id for a retry. A new sign-in starts a new view at the current head.
Names are public labels, **not authentication**; this example has no accounts,
presence or private rooms. Add real authorization in the front before exposing
private content.

`npm run build:starlingmonkey` builds the same six components with the
`-starlingmonkey.wasm` suffix. Both builds run `npm test` first and are picked up
by CI and `scripts/languages.sh` through their `examples/ts-*/` loop. The browser
bundle and HTML are embedded in the front; `src/assets.ts` is generated.

Deploy the QuickJS build (the default) for a real room. On StarlingMonkey,
the room actor can run out of guest memory when its node is short of CPU
(measured with one core); see the 0.6.0 known limits in the
[changelog](../../CHANGELOG.md).

The front accepts `GET /join`, `POST /publish`, `GET /history` and `GET /stats`,
with `?name=<chosen name>`. Publish takes `{ "text": "hello", "id": "retry-id" }`;
history accepts `after=<epoch>.<number>` and `limit` (1 to 256, the default),
and answers the JSON the room renders, at most 256 messages and about 64 KiB
of bodies a page; the browser client halves `limit` after a 5xx and keeps its
socket. Stats reports the head (from the room)
and persisted edge/relay counts (from the pusher); it does not claim to expose
the full ADR's runtime metrics.
The browser counters show delivered messages, repeated items discarded,
reconnect attempts, HTTP history requests, void numbers and resets. A reset
clears the view; void numbers are removed from it. Only 2,000 displayed messages
are retained.

Capacity is 150 per edge, with the SDK's normal minute hysteresis. A room grows
by splitting ranges and closing moved sockets with 4801. Relay fan-out begins
above 16 edges. A room deployed with 0.6.0 (four classes) moves its edges and
map to the pusher by itself once the five classes are deployed together; see
the [changelog](../../CHANGELOG.md). The room uses plain writes (`confirm`
defaults to false); on a
cluster, choose batched writes in deployment configuration for throughput, and
set `confirm: true` in `src/room.ts` when delivery must wait for upload. The
history window is bounded: a client away too long receives a reset, and lost
batched writes are explicitly announced as void after failover.

The real-node cluster proof lives in
[`room_cluster.rs`](../../crates/wasmkeep-server/tests/room_cluster.rs). It uses
Rust SDK fixtures, a shared in-memory object store, three independent node
runtimes, paced clients, split/rejoin, abrupt node death and a stalled reader.
Both the 30-client and 300-client proofs run in the default suite. To isolate
the large proof:

```bash
time cargo nextest run --build-jobs 4 --test-threads 4 -p wasmkeep-server \
  --test room_cluster -E 'test(room_cluster_300_clients)'
```

The fixture uses one-second scaling hysteresis to keep the test bounded; it
checks every retained number and 4801/4802 recovery. This is a correctness test,
not a throughput benchmark or the real-bucket 10,000-client proof (R7/R8).
