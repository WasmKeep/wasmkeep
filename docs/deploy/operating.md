# An upgrade, a verify and a trace

Three things an operator does on a running cluster, each walked through
once: upgrading WasmKeep one node at a time, checking that the bucket is
whole, and reading a trace. The examples use three nodes `a`, `b` and
`c`, one app `acme/shop` and the bucket `s3://wasmkeep-demo`. Every
command reads its bucket credentials from the `AWS_*` environment
variables.

The reference for each is elsewhere:
[Upgrading WasmKeep](../cluster-upgrades.md),
[Verifying a bucket](../cluster-operations.md#verifying-a-bucket) and
[Traces](../monitoring.md#traces). The page about a destructive command
that stopped half-way is
[Resuming destructive operations](../operations.md).

## 1. An upgrade, from 0.8.1 to 0.9.0

[Upgrade a cluster to 0.9.0](upgrading-to-0.9.md) is the rule for this
pair, with what must be true before you start. This is one pass through
its rolling way, one node at a time. For 0.9.0 the recommended way is to
stop every node and start them all on 0.9.0, then finalize as below: a
rolling upgrade is safe for data but, under load, not yet proven to meet
the latency goal ([measured](upgrading-to-0.9.md#measured)).

**Look first.** With the 0.9.0 `wasmkeep`:

```bash
wasmkeep cluster status --bucket s3://wasmkeep-demo
wasmkeep cluster upgrade status --bucket s3://wasmkeep-demo
```

Three nodes live on 0.8.1, and no floor record, which reads as 0.8:

```text
Upgrade of s3://wasmkeep-demo (this wasmkeep: 0.9)

acme/shop
  floor: none (0.8)
  state: one version
  every live node runs 0.8
  NODE  VERSION  LIVE
  a     0.8.1    yes
  b     0.8.1    yes
  c     0.8.1    yes
```

**Move node `a`.** Drain it, wait for `drained`, stop it, install 0.9.0,
withdraw the drain request, start it:

```bash
wasmkeep cluster drain a --bucket s3://wasmkeep-demo
wasmkeep cluster status --bucket s3://wasmkeep-demo      # until a shows STATE drained
sudo systemctl stop wasmkeep                             # on a's machine
# install the 0.9.0 binary on a's machine
wasmkeep cluster undrain a --bucket s3://wasmkeep-demo
sudo systemctl start wasmkeep                            # on a's machine
```

**Wait for `serving`.** `a` is back when `/_wasmkeep/ready` answers 200
on it and `cluster status` lists it live, on 0.9.0, with `STATE`
`serving`. If it does not come back, its log and its readiness answer
say why: a 0.9.0 node refuses to start beside a live node more than one
minor version away, and names that node.

The cluster is now mixed, which `upgrade status` says with exit code 3:

```text
acme/shop
  floor: none (0.8)
  state: mixed
  b (0.8.1), c (0.8.1) on 0.8, a (0.9.0) on 0.9
  blocks a finalize: b, c
```

`cluster status` warns that live nodes run different versions. Both are
expected until the last node. Deploys, secrets and the rest keep working
with the 0.9.0 `wasmkeep`; what 0.9.0 adds (a deploy with `apart-from` or
`spread`) is refused until the finalize.

**Do the same for `b`, then `c`.** After the last one:

```text
acme/shop
  floor: none (0.8)
  state: finalize pending
  every live node runs 0.9 and the floor is 0.8: run `wasmkeep cluster upgrade finalize` to end the upgrade (no node of an older version can start after it)
```

Up to here any node can go back to 0.8.1, by the same steps.

**Finalize.** First without `--yes`, which writes nothing and exits with
2:

```bash
wasmkeep cluster upgrade finalize --bucket s3://wasmkeep-demo
```

```text
acme/shop: would raise the upgrade floor from none (0.8) to 0.9; no node older than 0.9 could start after it, and going back would not be supported
Nothing written. Pass --yes to finalize.
```

Then for good:

```bash
wasmkeep cluster upgrade finalize --bucket s3://wasmkeep-demo --yes
```

It writes the floor, waits one start check (about 25 s), lists the
nodes again, and ends with `acme/shop: finalized at 0.9` and exit code
0. `upgrade status` now says `one version` and exits with 0.

**When finalize refuses:**

| It says | Do |
| --- | --- |
| `live nodes are older than 0.9: ...` | Upgrade or stop each node it names, then finalize |
| `nodes older than 0.9 are not live, but their lease was never released, nor fenced ...` | The node vanished without a clean stop. Make sure it is stopped, run `wasmkeep cluster fence <id>`, wait 15 s, finalize |
| `no node of this app is live: there is nothing to finalize` | Start the nodes, then finalize |
| `live nodes run a newer version than this ...` | Finalize with that version's `wasmkeep` |
| After the wait: `nodes older than the floor 0.9 are live ...`, exit code 1 | A 0.8.1 node started while the floor was written. Stop it. The floor stays |

## 2. A `cluster verify`

`wasmkeep cluster verify` checks that what the bucket holds is whole:
every actor can be restored from it, and the records around the actors
read. It sends no write, so read access is enough and it is safe on a
running cluster. Good moments for one: before an upgrade, after it, after
a bucket incident, and on a schedule.

**The quick pass** lists everything and reads one small object per
actor:

```bash
wasmkeep cluster verify --bucket s3://wasmkeep-demo
```

A report looks like this one, from a run with `--app acme/shop --deep`
on the small bucket of the test suite (hence `bodies read`, and the
time):

```text
Verifying s3://wasmkeep-demo, bodies read
app acme/shop: deployed 7d551a82e742945e (deploy 2), recorded-head format, floor 0.8
  actors: 4 with history, 4 restorable (34 objects, 17 KiB); 4 ownership records, 4 read; 4 read whole
  alarms: 2 entries; lease markers left: 4
nodes: 3 leases
Read 18 objects (400 KiB) and listed 47 keys in 38 requests, 0.0 s.
The bucket is whole.
```

**Read it from the bottom.** The last line is the verdict, and the exit
code says the same:

| Exit code | Meaning | Do |
| --- | --- | --- |
| 0 | The bucket is whole, and everything was read | Nothing |
| 1 | A problem was found | Read each `problem:` line: it names the key |
| 2 | No problem found, but no verdict: something was not read | Read the `not read:` lines, fix access or wait, run it again |

The line before the verdict is what the run cost: objects read, keys
listed, requests and time.

**A `problem:` line names one key** and says what is wrong with it. A
missing segment, for example, is named by the key it would have, with
what is built on it. From there:

- narrow the run to that actor, and read its history too:

  ```bash
  wasmkeep cluster verify --bucket s3://wasmkeep-demo --app acme/shop --actor room/lobby --deep
  ```

- if the actor cannot be restored from the bucket, the copy to bring it
  back from is a backup:
  [back up and restore the bucket](backup-and-restore.md).

**A `not read:` line is not damage.** It is a read the bucket refused
three times, an actor with more history keys than the listing budget
holds, or an app under `backup restore`. Run it again later.

**A `note:` line is neither.** It changes neither the verdict nor the
exit code. There is one today: a `wasmkeep cluster forget` that did not
finish left its mark at `nodes/<id>`, and no node starts under that id
until the command is run again.

**Go deeper on a schedule.** Without `--deep`, damage inside a history
object is not found: only missing, misnamed or cut objects are. `--deep`
reads every object a restore would read and checks its checksums, so it
costs a read of every actor's restorable history. `--sample N` bounds
that: the records and histories of N actors of each app, chosen at
random, while every actor's chain is still checked by its key names:

```bash
wasmkeep cluster verify --bucket s3://wasmkeep-demo --sample 100 --deep
wasmkeep cluster verify --bucket s3://wasmkeep-demo --json    # for a job: exit_code, whole, complete, problems, unread
```

On a bucket with access-based tiering, a deep run moves what it reads
back to the frequent tier.

It is not `cluster diagnose`, which tests what the bucket's API does on
scratch objects, nor `backup verify`, which checks a backup's own
contents.

## 3. Reading a trace

Tracing is off by default. Turn it on per node with a collector that
takes OTLP over HTTP:

```bash
wasmkeep serve ... --otlp-endpoint http://collector:4318 --trace-sample 0.01
```

In a cluster that is being upgraded, add these flags (or the `[process]`
keys `otlp-endpoint`, `trace-sample` and `trace-trust-incoming` of the
node file) only after the finalize, or remove them before a node goes
back to 0.8.1, which does not know them.

**What starts a trace.** A request from outside, an alarm, and each
socket message or close. With
`--trace-sample 0.01`, one in a hundred of them is sampled, at random. A client's
`traceparent` keeps its trace id; its sampled flag is honoured only with
`--trace-trust-incoming`. That decision travels with the trace: a node
run with the flag passes a client's sampled flag on to every node the
request reaches.

**What a trace holds.** Six kinds of span, all under the service name
`wasmkeep`, with the node's id as `service.instance.id`:

| Span | What it is | Attributes worth reading |
| --- | --- | --- |
| `request` | One HTTP request as the node received it | `method`, `route` (`front`, `actor`, `asset`, `file_link`), `status` |
| `front` | The app's front running for that request | `cpu_seconds`, `outcome` |
| `event` | One actor event | `tenant`, `app`, `node`, `class`, `actor`, `kind` (`request`, `call`, `alarm`, `socket_open`, `socket_message`, `socket_close`), `woken`, `mailbox_wait_seconds`, `cpu_seconds`, `outcome` |
| `call` | One `actors.call`, in the event or front that made it | `class`, `actor`, `remote` (a request for it left the node), `status`, `outcome` |
| `hop` | One request this node sent to another: a forward, a relayed WebSocket upgrade, a call, a placed request | `kind`, `node`, `peer`, `status`, `outcome`, `resent`, `carried` |
| `bucket` | One bucket request made while handling it | `operation` (`get`, `put`, `list`, `delete`, ...), `family` (`wal`, `snapshot`, `owners`, `alarms`, ...), `outcome` |

Spans carry actor names, and no request path, query string, header, body
or bucket key.

**Reading a slow request.** Take a request through the front to a room
actor that answered slowly, and read down the tree:

1. The `request` span is the whole time on this node, with the status
   the client got.
2. `front` is the front's own run. A long `front` beside a short
   `event` is the front's code: look at `cpu_seconds`.
3. An `event` span is one turn of an actor. `mailbox_wait_seconds` is how
   long the event waited behind others for that actor: a large value
   means the actor is busy, not slow
   ([designing for hot actors](../designing-for-hot-actors.md)).
   `woken` true means the actor was not in memory and was restored or
   reopened first.
4. Under the event, each `bucket` span is one request to the bucket.
   Several `get` spans of family `snapshot` and `wal` before the code
   ran are a restore from the bucket. A `put` of family `wal` is a
   write's upload: the answer to a durable write waits for it, so its
   length is the bucket's round trip.

Warnings and errors a node logs about a sampled request carry the
trace's id, as `trace_id`.

**Across nodes.** A request that crosses nodes is one trace: under each
`hop` span is the `request` span of the node it went to, and that node's
events and bucket requests under it. The node that starts a trace
decides whether it is sampled and the others follow, so a sampled trace
has its spans from every node. Two things to know:

- a node follows only nodes that have tracing on too. Run every node
  with the same tracing settings; with tracing on at some nodes only, or
  beside a 0.8.1 node during an upgrade, traces stop or split there;
- background work (renewals, sweeps, compaction, a drain's or a move's
  own requests to other nodes) is not traced.

The rules are in [ADR 0044](../adr/0044-traces.md#3-on-a-hop).

**Is the export healthy?** Watch
`wasmkeep_trace_spans_dropped_total` and
`wasmkeep_trace_exports_total{outcome="ok"|"error"}`. A slow collector
never blocks a request: spans are dropped instead.
[Traces](../monitoring.md#traces) has the queue sizes and what the
exporter does not do.
