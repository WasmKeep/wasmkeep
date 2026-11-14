# From one node to three

You will run three WasmKeep nodes on your machine, sharing one local
S3-compatible bucket. You will send requests through different nodes and
see one actor, kill a node and watch its actor come back on another with
its value, then drain a node and take it out.

This follows [Your first actor](first-actor.md). Applies to: WasmKeep
built from `main` (the release after `0.3.0`). Commands assume Linux and
one terminal: the nodes run in its background, and the variables you set
stay set. Run every command from the clone's directory.

## 1. Get the code

You need [rustup](https://rustup.rs) with the `wasm32-wasip2` target (as in
the first tutorial), `curl`, and [uv](https://docs.astral.sh/uv/) or `pip`
for the local bucket. Clone WasmKeep and build the server:

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

## 2. Build the counter

The actor is [`examples/counter`](../../examples/counter): `POST` adds one
and returns the new value, `GET` returns it. Build it with the `durable`
feature:

<!-- test: build-counter -->
```bash
cargo build --release --target wasm32-wasip2 --features durable \
  --manifest-path examples/counter/Cargo.toml
```

With `durable`, a `POST` answers only once the new value is in the bucket
(`exec-durable`). That is what lets the value survive a node killed right
after it answered. Without it, a write's upload follows the answer, and a
crash in between can lose it
([Replication](../cluster.md#replication)).

## 3. Start a bucket

[moto](https://github.com/getmoto/moto) runs an S3 API in memory, enough to
try a cluster. With uv:

<!-- test: moto-uvx -->
```bash
mkdir -p .wasmkeep/cluster
uvx --from 'moto[server]==5.2.5' moto_server -p 9000 > .wasmkeep/cluster/moto.log 2>&1 &
echo $! > .wasmkeep/cluster/moto.pid
```

Or with pip, install it once (`pip install 'moto[server]==5.2.5'`), then:

<!-- test: moto-pip -->
```bash
mkdir -p .wasmkeep/cluster
moto_server -p 9000 > .wasmkeep/cluster/moto.log 2>&1 &
echo $! > .wasmkeep/cluster/moto.pid
```

Wait for it, create a bucket named `demo`, and point WasmKeep at it:

<!-- test: bucket -->
```bash
until curl -sf -o /dev/null localhost:9000/moto-api/; do sleep 1; done
curl -X PUT -H 'Content-Length: 0' localhost:9000/demo
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
  AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true
```

Every `wasmkeep` command below reads the bucket's address and credentials
from these `AWS_*` variables. Against a real bucket, only they change
([Three nodes](../deploy/three-nodes.md#1-the-bucket)).

## 4. Deploy the app

Nodes started with `--app` run what was last deployed for that app, so
deploy first. The app is `demo/shop`, with one class, `counter`:

<!-- test: deploy -->
```bash
wasmkeep deploy --app demo/shop --bucket s3://demo \
  --actor counter=examples/counter/target/wasm32-wasip2/release/counter.wasm
```

You should see the class added and the new version (its id differs):

```text
  class counter: added
demo/shop: version 469fcd8d39aa32f2 is current (deploy 1)
```

## 5. Start three nodes

Each node has its own id, data directory, client port (8081 to 8083) and
cluster port (7481 to 7483, where the nodes talk to each other over mutual
TLS):

<!-- test: start-nodes -->
```bash
wasmkeep serve --app demo/shop --bucket s3://demo --node-id a \
  --data-dir .wasmkeep/cluster/a --listen 127.0.0.1:8081 --advertise http://127.0.0.1:8081 \
  --cluster-listen 127.0.0.1:7481 --cluster-advertise https://127.0.0.1:7481 \
  > .wasmkeep/cluster/a.log 2>&1 &
echo $! > .wasmkeep/cluster/a.pid
wasmkeep serve --app demo/shop --bucket s3://demo --node-id b \
  --data-dir .wasmkeep/cluster/b --listen 127.0.0.1:8082 --advertise http://127.0.0.1:8082 \
  --cluster-listen 127.0.0.1:7482 --cluster-advertise https://127.0.0.1:7482 \
  > .wasmkeep/cluster/b.log 2>&1 &
echo $! > .wasmkeep/cluster/b.pid
wasmkeep serve --app demo/shop --bucket s3://demo --node-id c \
  --data-dir .wasmkeep/cluster/c --listen 127.0.0.1:8083 --advertise http://127.0.0.1:8083 \
  --cluster-listen 127.0.0.1:7483 --cluster-advertise https://127.0.0.1:7483 \
  > .wasmkeep/cluster/c.log 2>&1 &
echo $! > .wasmkeep/cluster/c.pid
```

Wait until all three are ready:

<!-- test: wait-ready -->
```bash
for port in 8081 8082 8083; do
  until curl -sf -o /dev/null localhost:$port/_wasmkeep/ready; do sleep 1; done
  echo "$port ready"
done
```

This takes a little over 15 s. Before it takes any actor, a node checks
for 15 s that no other process runs with its id
([why](../cluster.md#flags)). Each node logs to its file in
`.wasmkeep/cluster/`: `checking that no other process runs node id a;
ready in about 15 s` when it starts, then `no other process runs node id
a: ready to take actors`.

## 6. One actor, any node

Add one to the counter `alice` through node `a`:

<!-- test: first-post -->
```bash
curl -i -X POST localhost:8081/actor/counter/alice
```

```text
HTTP/1.1 200 OK
content-type: text/plain
x-wasmkeep-node: a
...

1
```

`x-wasmkeep-node` names the node that runs the actor. The first request
started `alice` on the node it reached, `a`. Now go through the other two:

<!-- test: other-nodes -->
```bash
curl -X POST localhost:8082/actor/counter/alice
# 2
curl -X POST localhost:8083/actor/counter/alice
# 3
curl -si localhost:8083/actor/counter/alice | grep -i x-wasmkeep-node
# x-wasmkeep-node: a
```

There is one `alice`, on `a`. Nodes `b` and `c` forwarded your requests to
it. A load balancer in front can send a request to any node
([the front door](../deploy/front-door.md)).

## 7. Read the cluster status

`wasmkeep cluster status` reads the bucket alone, so it works whether or
not a node answers:

<!-- test: status -->
```bash
wasmkeep cluster status --bucket s3://demo
```

```text
Cluster s3://demo (live: heartbeat within 10.0s)

NODE  LIVE  AGE    STATE    ACTORS  SOCKETS  EV/MIN  VERSION  APP        ADDRESS                CLUSTER ADDRESS         TLS
a     yes   2.0s   serving  1       0        -       0.3.0    demo/shop  http://127.0.0.1:8081  https://127.0.0.1:7481  pinned
b     yes   2.0s   serving  0       0        -       0.3.0    demo/shop  http://127.0.0.1:8082  https://127.0.0.1:7482  pinned
c     yes   1.9s   serving  0       0        -       0.3.0    demo/shop  http://127.0.0.1:8083  https://127.0.0.1:7483  pinned

APP        DEPLOYED VERSION  SEQ  ACTORS
demo/shop  469fcd8d39aa32f2  1    1

3 of 3 nodes live; TLS: 3 with a cluster listener, 0 plain; version: 0.3.0
```

- **LIVE** is `yes` when the node's heartbeat in the bucket is recent;
  **AGE** is how old it is.
- **STATE** is `serving`, or `draining` and `drained` (step 10); `-` for
  a node that is not live.
- **ACTORS**, **SOCKETS** and **EV/MIN** are each node's load as of its
  last heartbeat: actors in memory, open WebSockets, events per minute.
  They lag at most one heartbeat (3 s), and EV/MIN is `-` for a node's
  first minute.
- **TLS** `pinned`: the other nodes know this node's certificate and talk
  to it over mutual TLS.
- The app table shows the deployed version and its actors on live nodes.

The command exits with 1 when no node is live, so scripts can use it;
`--json` prints the same for them
([Checking a cluster](../cluster.md#checking-a-cluster)).

## 8. Kill a node

An actor left alone for 30 s is put to sleep (hibernated) and its node
gives it up. Then a request starts it on whichever node it reaches, and a
crash shows nothing. So touch `alice` first, then kill `a` at once:

<!-- test: kill -->
```bash
curl -X POST localhost:8081/actor/counter/alice
# 4
kill -9 $(cat .wasmkeep/cluster/a.pid)
```

`a` had no time to hand anything over. Try to add one through `b`:

<!-- test: post-after-kill -->
```bash
curl -i -X POST localhost:8082/actor/counter/alice
```

```text
HTTP/1.1 503 Service Unavailable
retry-after: 1
...

the actor's node did not answer; retry
```

No node may run `alice` yet: `a` might only be slow, and two nodes must
never write one actor. A `POST` is refused at once, and nothing ran. Read
it instead:

<!-- test: get-after-kill -->
```bash
time curl -si localhost:8082/actor/counter/alice | grep -iE '^x-wasmkeep-node|^[0-9]'
```

```text
x-wasmkeep-node: b
4
```

This one waits, then answers `4` from `b` or `c`: the value survived. A
`GET` without a body is kept until a node takes the actor over, 15 to
about 21 s after `a`'s last heartbeat (shortly before the kill), plus
restoring the database from the bucket. `time` shows the wait. The node
that took `alice` over now writes it:

<!-- test: post-after-failover -->
```bash
curl -X POST localhost:8083/actor/counter/alice
# 5
wasmkeep cluster status --bucket s3://demo
```

In the status, `a` is `no (fenced)`, with `-` for its state and load: a
peer fenced its heartbeat to take its actors, and the summary reads `2 of
3 nodes live`. What callers get
during the wait, and why it lasts that long:
[When a node dies](../failover-and-scaling.md#a-node-that-dies).

## 9. Restart it

Start `a` again with the same command:

<!-- test: restart-a -->
```bash
wasmkeep serve --app demo/shop --bucket s3://demo --node-id a \
  --data-dir .wasmkeep/cluster/a --listen 127.0.0.1:8081 --advertise http://127.0.0.1:8081 \
  --cluster-listen 127.0.0.1:7481 --cluster-advertise https://127.0.0.1:7481 \
  > .wasmkeep/cluster/a.log 2>&1 &
echo $! > .wasmkeep/cluster/a.pid
until curl -sf -o /dev/null localhost:8081/_wasmkeep/ready; do sleep 1; done
```

It is ready sooner this time: it was fenced, so it does not wait the full
15 s. It rejoins and takes new actors. `alice` stays where it is:

<!-- test: after-restart -->
```bash
curl -si -X POST localhost:8081/actor/counter/bob | grep -iE '^x-wasmkeep-node|^[0-9]'
# x-wasmkeep-node: a
# 1
curl -si localhost:8081/actor/counter/alice | grep -iE '^x-wasmkeep-node|^[0-9]'
# x-wasmkeep-node: b   (or c, whichever took it over)
# 5
```

## 10. Drain a node

To take a node out of service, drain it first. It stops taking actors and
hands its own to the other nodes, while it keeps forwarding requests. `a`
holds `bob`:

<!-- test: drain -->
```bash
wasmkeep cluster drain a --bucket s3://demo
```

```text
Asked a to drain; it reports `draining`, then `drained` once its actors are handed over (a node with few actors may go straight to `drained`; see `wasmkeep cluster status`).
```

Wait for `drained` in the STATE column:

<!-- test: wait-drained -->
```bash
until wasmkeep cluster status --bucket s3://demo | grep -q '^a .* drained '; do sleep 1; done
wasmkeep cluster status --bucket s3://demo
```

```text
NODE  LIVE  AGE    STATE    ACTORS  ...
a     yes   1.2s   drained  0       ...
```

A drain this small can go from `serving` to `drained` between two
heartbeats, so you may never see `draining`. `bob` moved with its value:

<!-- test: after-drain -->
```bash
curl -si localhost:8081/actor/counter/bob | grep -iE '^x-wasmkeep-node|^[0-9]'
# x-wasmkeep-node: b   (or c)
# 1
```

`a` holds nothing now, so stopping it hands nothing over:

<!-- test: stop-drained -->
```bash
kill $(cat .wasmkeep/cluster/a.pid)
```

A few seconds later the status shows `a` as `no (released)`, with `-`
for its state: a clean stop.

<!-- test: status-after-stop -->
```bash
wasmkeep cluster status --bucket s3://demo
```

To keep a drained node instead, `wasmkeep cluster undrain a
--bucket s3://demo` lets it take actors again
([Draining a node](../cluster.md#draining-a-node)).

## 11. Clean up

Stop the other nodes and the bucket, wait for them to exit, and remove
what this page wrote:

<!-- test: cleanup -->
```bash
kill $(cat .wasmkeep/cluster/b.pid) $(cat .wasmkeep/cluster/c.pid) $(cat .wasmkeep/cluster/moto.pid)
wait
rm -rf .wasmkeep/cluster
```

moto kept the bucket in memory, so it is gone too.

## What just happened

- **One owner per actor.** A node runs each actor it holds; the others
  forward to it. Ownership is a lease in the bucket, so the bucket is the
  only thing the nodes share.
- **A crash costs a wait, not data.** A killed node's actors are taken over
  once its lease runs out and a peer fences it. Writes made with
  `exec-durable` were in the bucket before they were acknowledged.
- **A drain or a clean stop costs almost nothing.** The node hands its
  actors over itself, so the next request starts them elsewhere at once.

## Next

- [Running a cluster](../cluster.md): how ownership, forwarding and
  replication work, every flag, deploys and rollbacks.
- [When a node dies, and how it scales](../failover-and-scaling.md): what
  callers get during a failover, alarms, adding nodes.
- [Monitoring](../monitoring.md): health and readiness checks, metrics,
  alerts.
- [Three nodes](../deploy/three-nodes.md): the same cluster on three
  servers behind a load balancer.
