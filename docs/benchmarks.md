# Benchmarks

Every number here comes from a file written by `./bench/run.sh` (or
`./bench/soak.sh`, for a real bucket), named in its section; sections that name no file use
[`bench/results/2026-11-04-db85998.csv`](../bench/results/2026-11-04-db85998.csv)
(commit `db85998`), except the failover row, which needs an S3 API and comes
from [`2026-11-03-7060271.csv`](../bench/results/2026-11-03-7060271.csv).
The short hash in a measurement file's name (and in the text below)
identifies the build that was measured; it is a label, not a commit in this
repository. Anyone can rerun it and get their own file; numbers that do not come from
such a file do not belong in the docs.

**Hardware:** a shared cloud container (KVM, 4 vCPU Xeon @ 2.10 GHz, 16 GB),
Rust 1.99.1, Wasmtime 50.0.1, Node 22, workerd 2026-10-16. A shared VM is
noisy (we saw ±30% between runs on some rows), so read these as orders of
magnitude, not decimals. Compared with the previous file
([`2bfc65f`](../bench/results/2026-11-04-2bfc65f.csv)), workerd's CPU per
request moved 2%, so the machine was comparable; WasmKeep's Rust counter
went from 295 to 203 µs of CPU per request and from 9,897 to 15,356 req/s,
after the host stopped hopping to the blocking pool for its own
bookkeeping. The wake row came out slower (1.07 to 1.39 ms), but three
interleaved runs of both commits on the same machine overlapped (1.47 to
1.98 ms after, 1.73 to 1.87 ms before): that row is noise, not a change.
Publishable comparisons belong on dedicated
hardware: see the [benchmark showdown](../ROADMAP.md#stretch-goal-the-benchmark-showdown).

## WasmKeep against workerd, the same counter

The same actor in three runtimes: one counter object per name, its value in
SQLite, `GET` reads and `POST` increments.

- **WasmKeep, Rust**: [`examples/counter`](../examples/counter), compiled ahead of time.
- **WasmKeep, TypeScript**: [`examples/ts-counter`](../examples/ts-counter) on QuickJS, the default engine, embedded in the component.
- **workerd**: Cloudflare's open-source Workers runtime with a SQLite-backed Durable Object, [`bench/workerd/counter.js`](../bench/workerd/counter.js).

Each runtime runs as its own server process, and the same client
([`bench/compare`](../bench/compare)) drives it over loopback HTTP/1.1
keep-alive. Server memory and CPU time come from `/proc/<pid>`.

From [`bench/results/2026-11-06-351ea57.csv`](../bench/results/2026-11-06-351ea57.csv)
(commit `351ea57`, a clean tree, the first run with transparent huge
pages off for the server, see below; the previous run,
[`dec1a82`](../bench/results/2026-11-06-dec1a82.csv), was within a few
percent on every row but memory). **Hardware:** an AMD Ryzen 9 7950X
workstation (16 cores, 32 threads), Rust 1.99.1, Wasmtime 50.0.1, Node 26,
workerd 1.20261016.1. Nothing else was running. Both runtimes kept their
actor data on tmpfs (`/tmp`), so no disk is involved on either side.

| Counter | WasmKeep, Rust | WasmKeep, TypeScript | workerd |
| --- | --- | --- | --- |
| Read, resident actor (p50 / p99) | 61 / 87 µs | 139 / 269 µs | 156 / 212 µs |
| Write, resident actor (p50 / p99) | 67 / 92 µs | 146 / 262 µs | 177 / 225 µs |
| First request to a new actor (p50) | 0.49 ms | 0.77 ms | 0.41 ms |
| Memory per resident actor | 250 KB | 741 KB | 277 KB |
| **Server CPU time per request** (8 clients) | **104 µs** | 230 µs | 128 µs |
| Throughput, 8 clients, 1000 actors | 61,963 req/s | 29,712 req/s | 7,681 req/s* |
| Process start to first response | 51 ms | 202 ms | 51 ms |

\* **workerd runs one thread per process by design**: Cloudflare scales it with
many processes. Its throughput row measures one process, which is not a fair
comparison. **CPU time per request** is the fair efficiency comparison, and
the best proxy for energy we can measure here.

The same comparison on a shared 4-vCPU cloud VM
([`2026-11-05-ae5416c.csv`](../bench/results/2026-11-05-ae5416c.csv)) gave
a different picture:

| On the 4-vCPU VM | WasmKeep, Rust | workerd |
| --- | --- | --- |
| Server CPU time per request | 215 µs | 555 µs |
| Memory per resident actor | 251 KB | 267 KB |
| Read, resident actor (p50) | 213 µs | 411 µs |

### How to read it

- **The machine changes the answer.** On the workstation, WasmKeep with Rust
  uses about 81% of workerd's CPU per request; on the 4-vCPU VM it used 39%.
  workerd gains much more from fast cores (555 to 128 µs) than WasmKeep
  does (215 to 104 µs). A claim like "less than half the CPU" holds on one
  machine and not on the other, so quote both or neither.
- **WasmKeep with Rust is still the fastest per request here.** Reads and
  writes at p50 are about 2.6× faster than workerd's, and the p99 gap is
  2.4×.
- **Memory per resident actor is about workerd's**: 250 KB against
  277 KB, as on the VM (251 KB). Earlier workstation runs read 571 to
  577 KB (files `cd48f41` to `dec1a82`), for every native language alike:
  this machine's kernel has transparent huge pages set to `always`, so
  each actor's memory was backed by 2 MiB pages it mostly did not use.
  `wasmkeep serve` now turns them off for its own process
  (`--transparent-huge-pages system` keeps the kernel's setting); CPU per
  request and latency did not move (three runs each way, within 2%).
- **TypeScript on WasmKeep costs more than Rust.** On QuickJS, the default
  engine, it uses about 2.2× the CPU of Rust and 3× the memory per
  resident actor (1,000 resident actors: 936 MB), and about 1.8×
  workerd's CPU. Every actor carries its own JavaScript engine, without
  V8's JIT: for JavaScript-heavy workloads, workerd is the better home
  (QuickJS takes 5.8 ms for the `js_work` load below). TypeScript on
  WasmKeep is for teams that want WasmKeep's model in their language. It is
  not a speed play. StarlingMonkey costs more, see below.
- **Writes are not exactly equivalent.** A Durable Object's storage may wait
  for the disk before answering a write, while WasmKeep's plain `exec` commits
  without an fsync. On tmpfs neither waits for a disk, so the write row is
  closer to fair here than on a real disk. A durable write on WasmKeep
  (`exec-durable`, replicated) costs under 2 ms with an in-memory bucket,
  see below.
- **Not measured yet:** idle memory after hibernation (WasmKeep's idle actors
  hold none), wake latency per runtime, and workerd with several processes.
  These are next for the comparison harness.

## The same counter in every language

The same actor contract (read, increment, persist) in every guest language
WasmKeep supports, each runtime its own server process driven by the same
client as above. From
[`bench/results/2026-11-06-351ea57.csv`](../bench/results/2026-11-06-351ea57.csv)
(commit `351ea57`, the workstation and run of the workerd table above).
TypeScript is on QuickJS, the default engine; StarlingMonkey is the
`--engine starlingmonkey` build of the same source.

| Counter | Rust | Zig | C | Go (TinyGo) | TypeScript | TS on StarlingMonkey | Python |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Component | 81 KB | 30 KB | 170 KB | 151 KB | 1.7 MB | 13.4 MB | 18.7 MB |
| Server CPU per request (8 clients) | 104 µs | 107 µs | 110 µs | 248 µs | 230 µs | 368 µs | 259 µs |
| Throughput, 8 clients | 61,963 req/s | 59,649 req/s | 58,840 req/s | 28,160 req/s | 29,712 req/s | 19,708 req/s | 26,751 req/s |
| Read, resident actor (p50 / p99) | 61 / 87 µs | 72 / 116 µs | 65 / 114 µs | 73 / 742 µs | 139 / 269 µs | 227 / 334 µs | 142 / 212 µs |
| Write, resident actor (p50 / p99) | 67 / 92 µs | 76 / 111 µs | 72 / 124 µs | 85 / 3,375 µs | 146 / 262 µs | 241 / 329 µs | 151 / 214 µs |
| Memory per resident actor | 250 KB | 242 KB | 244 KB | 390 KB | 741 KB | 3.4 MB | 7.8 MB |
| First request to a new actor (p50) | 0.49 ms | 0.51 ms | 0.51 ms | 0.70 ms | 0.77 ms | 1.2 ms | 1.8 ms |
| Process start to first response | 51 ms | 51 ms | 51 ms | 52 ms | 202 ms | 704 ms | 705 ms |

The same table on the shared 4-vCPU cloud VM
([`2026-11-05-ae5416c.csv`](../bench/results/2026-11-05-ae5416c.csv),
without QuickJS) had the same order at 1.5 to 3.4× this machine's CPU per
request: Rust 215 µs, Zig 214, C 222, Go 364, TypeScript on StarlingMonkey
855, Python 859.

### How to read it

- **Rust, Zig and C cost the same.** Their CPU per request is within 6% of
  each other, which is run-to-run noise here: most of a request's cost is
  the host (HTTP, entering and leaving Wasmtime, SQLite), not the guest.
  Zig's smaller component changes deploys and the first compile, not
  requests.
- **Zig needs its stack set.** Zig's default wasm stack is 16 MiB, and a
  component reserves it in every actor's memory: the Zig counter took
  2.66 MB per resident actor in an earlier run (with huge pages on),
  against 0.57 MB for C. The examples now build with `--stack 1048576`, as
  Rust's 1 MiB ([`sdks/zig`](../sdks/zig/README.md)).
- **TinyGo costs about 2.4× the CPU of the native three, with long tails.**
  Its median latency is close to theirs, but its p99 is 10× (reads) to 40×
  (writes) its median: TinyGo's garbage collector runs inside the request.
  Memory per actor is 1.6× theirs.
- **The embedded runtimes cost 2.2 to 3.5× the CPU.** QuickJS (230 µs) and
  CPython (259 µs) land close together, StarlingMonkey (368 µs) above them.
  Memory per resident actor is 3× (QuickJS), 14× (StarlingMonkey) and
  31× (CPython) Rust's: each actor carries its own engine or interpreter.
  1,000 resident Python actors took 8.4 GB.
- **Python's CPU per request fell the most from the VM** (859 to 259 µs,
  3.4×, against 2.1× for Rust): CPython's interpreter loop gains more from
  this machine's caches and clock than the host does.
- Component sizes are from each example's `build.sh` ([languages](languages.md)).

## TypeScript on QuickJS against StarlingMonkey

The same TypeScript counter ([`examples/ts-counter`](../examples/ts-counter)),
one source file built twice: with QuickJS, the default engine since
[ADR 0018](adr/0018-quickjs-the-default-typescript-engine.md)
(`npm run build`), and with StarlingMonkey (`npm run build:starlingmonkey`,
see [languages](languages.md#typescript-on-quickjs)), next to the Rust
counter. From the same file and run as the tables above,
[`2026-11-06-351ea57.csv`](../bench/results/2026-11-06-351ea57.csv). The
`js_work` rows time the counter's `/work` path
([`work.ts`](../examples/ts-counter/src/work.ts)): plain JavaScript
(building 200 records, a JSON round trip, filtering, sorting, string
formatting, a regex, a hash; four rounds), one request at a time, so its
latency is the engine's time.

| Counter | Rust | TypeScript, QuickJS | TypeScript, StarlingMonkey |
| --- | --- | --- | --- |
| Component | 81 KB | 1.7 MB | 13.4 MB |
| **Memory per resident actor** | 250 KB | **741 KB** | 3.4 MB |
| Server memory, 1,000 resident actors | 301 MB | 936 MB | 4.17 GB |
| **Server CPU per request** (8 clients) | 104 µs | **230 µs** | 368 µs |
| Throughput, 8 clients, 1,000 actors | 61,963 req/s | 29,712 req/s | 19,708 req/s |
| **Plain JavaScript work, `js_work` (p50 / p99)** | | 5.76 / 6.23 ms | **3.76 / 4.21 ms** |
| First request to a new actor (p50) | 0.49 ms | 0.77 ms | 1.22 ms |
| Process start to first response (empty compile cache) | 51 ms | 202 ms | 704 ms |

The comparison that decided the default
([`2026-11-06-dec1a82.csv`](../bench/results/2026-11-06-dec1a82.csv),
with huge pages on: 1.06 MB against 3.7 MB per actor) had the same CPU
and `js_work` rows within 7%.

### How to read it

- **QuickJS takes 4.6× less memory per resident actor** than StarlingMonkey
  (741 KB against 3.4 MB), 3× Rust's. A node holding 1,000 TypeScript
  actors needs 936 MB instead of 4.17 GB.
- **It costs less CPU on the counter:** 230 µs per request against 368 µs
  (37% less), 2.2× Rust instead of 3.5×. Each request's cost here is
  mostly entering a fresh instance, the host and SQLite, and a smaller
  engine is cheaper to instantiate.
- **StarlingMonkey is 1.5× faster on plain JavaScript work** (3.76 ms
  against 5.76 ms). Neither engine has a JIT; SpiderMonkey's interpreter
  is faster than QuickJS's. An actor that spends milliseconds computing in
  JavaScript per event is the case for `--engine starlingmonkey` (or for
  Rust).
- **Components are 8× smaller** (1.7 MB), which shortens deploys and the
  first compile on a node (202 ms against 704 ms from start to first
  response with an empty cache).
- What QuickJS does not provide (Web APIs such as `fetch`, `URL`, `crypto`,
  `console`, timers) is listed in [languages](languages.md#typescript-on-quickjs).

## Between nodes: plain HTTP against mutual TLS

What a hop between two nodes costs, and what [mutual TLS between
nodes](cluster.md) adds to it
([ADR 0011](adr/0011-node-to-node-mtls.md)). From
[`bench/results/2026-11-05-d8e9ab7.csv`](../bench/results/2026-11-05-d8e9ab7.csv)
(commit `d8e9ab7`, the `cluster/*` rows), with an earlier run of the same
suite,
[`2026-11-05-b07a9d7.csv`](../bench/results/2026-11-05-b07a9d7.csv), for
the spread, both on **different hardware** from the sections above: a
workstation (AMD Ryzen 9 7950X, 16 cores / 32 threads, 64 GB), Rust
1.99.1, data in a RAM-backed `/tmp` (tmpfs). Compare
these rows with each other, not with the tables above.

The suite ([`benches/cluster.rs`](../crates/wasmkeep-server/benches/cluster.rs))
runs two pairs of nodes in one process over loopback, each pair sharing an
in-memory bucket: one pair plain, one with cluster listeners. In the TLS
pair every node publishes a plain address nobody listens on, so a hop in
plaintext fails the run instead of being measured. One client, one
request at a time, kept alive; nodes pool their connections to each other.

| One request, median | Plain | Mutual TLS | TLS adds (earlier run) |
| --- | --- | --- | --- |
| Counter GET sent to the owner (no hop) | 63.2 µs | | |
| The same GET sent to the other node, which forwards it | 106.3 µs | 115.1 µs | 8.8 µs (11.8 µs) |
| `actors.call` from an actor on one node to an actor on the other | 133.7 µs | 155.1 µs | 21.4 µs (15.0 µs) |

The earlier run predates the change that made forwarding drop hop-by-hop
headers, which only adds a header filter to these kept-alive rows. Between the runs, rows moved by up to 7 µs
either way: read the TLS cost as a range, not a decimal.

### How to read it

- **A hop costs about 40 µs on loopback** in plain HTTP (106.3 against
  63.2 µs), **about 50 µs over TLS**. On a real network the round trip
  between the nodes comes on top, the same with or without TLS.
- **TLS adds about 10 to 20 µs per hop** (9 to 12 µs to a forwarded
  request, 15 to 21 µs to a call, across the two runs): encryption both ways, and the checks
  ADR 0011 makes on every request (the peer's pin checked again, and the
  answer checked before it is delivered). The call row also carries the
  signature and the body's hash, plain or not.
- **Not measured: a new connection.** Each row reuses a pooled connection,
  so a full TLS 1.3 handshake (with a client certificate, and no session
  resumption by design) is not in these numbers. Connections are pooled
  and kept alive, also for clients that send `Connection: close`
  (forwarding drops hop-by-hop headers); they are rebuilt when a peer's
  pin changes or its heartbeat lapses.
- Only one request at a time: throughput and CPU per request between
  nodes, and many clients at once, are not measured yet.

## Restore against history length

How long a node takes to restore an actor it has never run (as on
failover, or when an actor wakes on another node) as the actor's history
in the bucket grows, and what garbage collection changes. From
[`bench/results/2026-11-05-4c8b21a.csv`](../bench/results/2026-11-05-4c8b21a.csv)
(commit `4c8b21a`, the `restore/*` and `list/*` rows; the first run,
before collection existed, is
[`2026-11-05-cd48f41.csv`](../bench/results/2026-11-05-cd48f41.csv)), on the same
workstation as the section above (AMD Ryzen 9 7950X, 16 cores / 32
threads), Rust 1.99.1, data in a RAM-backed `/tmp` (tmpfs), and an
**in-memory bucket**. Compare these rows with each other, not with the
tables above.

The suite ([`benches/restore.rs`](../crates/wasmkeep-server/benches/restore.rs))
runs two nodes sharing the bucket over loopback. A writer gives one
counter N replicated writes (one segment each, plus one for `on-init`); a
restorer is then asked for fresh actors whose prefix holds a copy of that
history. Each timed request takes the lease, lists the prefix, plans,
downloads and applies the chain, and answers a counter GET. Three shapes
of the same history:

- **As written**: the owner uploads a snapshot every 64 segments, so a
  restore replays fewer than 64 segments, but nothing is ever deleted, so
  the prefix keeps every segment and snapshot.
- **Without periodic snapshots** (removed from the copy, the first kept):
  every segment is replayed. This is what an actor whose periodic
  snapshots kept failing would leave (a failed one is logged and retried
  64 segments later).
- **After garbage collection** ([ADR 0012](adr/0012-bucket-garbage-collection.md)):
  what the server's rule (`collectable`) leaves after the last periodic
  snapshot once the retention window has passed: that snapshot, the
  segment at its position and the segments after it.

| Writes (N) | Objects, as written | Replayed | Restore, as written | Restore, no periodic snapshots (replays N+1) | Listing, as written | Objects, collected | Restore, collected | Listing, collected |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| none | 0 | 0 | 0.78 ms | | | | | |
| 10 | 12 | 11 | 1.06 ms | 1.06 ms | 1.1 µs | 12 | 1.06 ms | 1.1 µs |
| 100 | 103 | 37 | 1.93 ms | 4.26 ms | 9.1 µs | 39 | 1.92 ms | 3.4 µs |
| 1,000 | 1,017 | 41 | 2.56 ms | 35.0 ms | 94.8 µs | 43 | 2.07 ms | 3.7 µs |
| 10,000 | 10,158 | 17 | 6.13 ms | 342 ms | 1.72 ms | 19 | 1.23 ms | 1.7 µs |

Medians. The baseline with no history, run again after all the other
restore rows, was 0.78 ms both times: restored actors piling up on the
node do not inflate the later rows. At 10 writes nothing is collected:
the only snapshot is the first one, with no segment at its position.

### How to read it

- **Periodic snapshots bound the replay, collection bounds the listing.**
  As written, a restore replays at most 63 segments whatever the history's
  length, but the prefix grows by one object per write and one per 64
  writes: at 10,000 writes a restore takes 6.1 ms against 2.6 ms at 1,000
  while replaying fewer segments (17 against 41), because it lists the
  whole prefix twice and listing 10,158 objects takes 1.7 ms in memory
  (restores now list the prefix once; not remeasured).
- **After collection the prefix holds at most 65 objects**, whatever the
  history's length (a snapshot and up to 64 segments): at 10,000 writes
  the restore takes 1.2 ms instead of 6.1 ms, and listing takes 1.7 µs
  instead of 1.7 ms. The collected rows stay within the range of small
  histories (1.1 to 2.1 ms here, depending on how many segments follow the
  last snapshot). On S3 or R2 the difference is larger: one listing page
  instead of eleven.
- **A replayed segment costs about 34 µs** here (342 ms for 10,001
  segments): one GET, decoding and applying it. Each counter write changes
  one page, so this is the fixed cost per segment; segments with more
  pages cost more.
- **Not measured: a real bucket.** The in-memory bucket answers a listing
  in one call; S3 and R2 page listings by 1,000 keys, so at 10,158 objects
  each listing is 11 requests (22 per restore in the version measured
  above, which listed twice), before the first download.
  At 20 to 50 ms per request that would add about 0.4 to 1.1 s to a
  restore (an estimate from the request count, not a measurement), and
  every replayed segment is one more GET: 82 ms each at the median next
  to OVHcloud Object Storage, so 1 to 6 s for up to 64 segments
  ([measured](#three-nodes-against-a-real-bucket)). LIST and GET counts against a
  real bucket, peak RSS during restore, larger databases and pages per
  segment, and many actors at once are not measured either. The
  collected rows are histories collected by the rule, copied into the
  bucket; the server's own passes (`--history-retention-secs`, an hour by
  default) are tested in `tests/cluster/`, not timed here.

## Restart and alarm recovery against actor count

How a node's restart and the recovery of a dead node's alarms scale with
the number of actors. From
[`bench/results/2026-11-05-4c10a33.csv`](../bench/results/2026-11-05-4c10a33.csv)
(commit `4c10a33`, the `restart/*` and `alarm_recovery/*` rows), on a
workstation (AMD Ryzen 9 7950X, 16 cores / 32 threads, 64 GB), Rust
1.99.1, data in a RAM-backed `/tmp` (tmpfs), an **in-memory bucket**,
and other work running on the machine at the same time. Compare these
rows with each other, not with the tables above.

### Restarting a node

[`benches/restart.rs`](../crates/wasmkeep-server/benches/restart.rs): one
standalone node holds N test actors, each created by a request that set
an alarm an hour out, so each database is as an actor leaves it (36 KB
on disk per actor with its WAL files: 3.7 GB at 100,000). The node is
stopped as a crash stops it and started again on the same directory.

| Actors on disk | Start-up alarm scan | Start to first answer from an actor | Start until every overdue alarm ran |
| --- | --- | --- | --- |
| 1,000 | 108 ms | 3.9 ms | 257 ms |
| 10,000 | 1.06 s | 3.9 ms | 3.36 s |
| 100,000 | 11.5 s | 4.0 ms | not measured |

Medians. The scan column is `Storage::pending_alarms` alone (the node
runs it at start-up on a blocking thread, then arms what it found). The
last column makes every actor's alarm overdue while the node is down, and
counts until each has run `on-alarm` once (it includes the scan).

- **A restart serves at once, whatever the population.** The listener
  does not wait for the scan: 4 ms from start to an actor's answer at
  100,000 actors as at 1,000.
- **The scan is linear, about 110 µs per database, on one thread.** It
  opens every database (with the same set-up as an actor's: WAL mode,
  pragmas, the host's tables) to read one row. Until it finishes, no
  alarm from before the restart is armed: at 100,000 actors an alarm due
  during the first 11.5 s fires up to 11.5 s late. At this rate a million databases would take about two
  minutes (an extrapolation; not measured, and 37 GB at this size). A
  slower disk than tmpfs adds its own cost per open.
- **Overdue alarms after a restart run at about 3,000 a second** (10,000
  in 3.4 s, of which 1.1 s is the scan): each one wakes its actor, runs
  the event, and the actor stays resident until the pool needs room.

### Alarm recovery after a node dies

[`benches/alarm_recovery.rs`](../crates/wasmkeep-server/benches/alarm_recovery.rs):
three nodes share the bucket. Node `a` creates N test actors, each with
an alarm at the same instant `T`, and is killed once the bucket holds
them and their alarm index entries (no lease released). Nodes `b` and `c`
never ran those actors: their sweeps find the alarms in the bucket's
index, and each delivery takes the actor over, restores it and runs
`on-alarm`. The time is from `T` until every alarm has run and the index
is empty. **Timings are a tenth of the defaults** (lease ttl 1 s, grace
0.5 s, heartbeat 0.3 s, sweep every 1 s instead of 10 s): an alarm can be
delivered by another node once it is overdue by a lease period plus
grace (1.5 s here, 15 s by default).

| Alarms left behind | Until all ran, at a tenth of the defaults | At the defaults (estimate: ×10) |
| --- | --- | --- |
| 100 | 1.92 s | about 19 s |
| 1,000 | 3.08 s | about 31 s |
| 10,000 | 23.2 s (38.2 s before the fix below) | about 3.9 min |

Medians. The 10,000 row is from
[`bench/results/2026-11-05-9c495b4.csv`](../bench/results/2026-11-05-9c495b4.csv)
(commit `9c495b4`, same machine, after the sweep fix; its samples ranged
from 23.19 to 23.23 s, and 100 and 1,000 measured the same as before);
38.2 s is the `4c10a33` run (37.2 to 38.2 s). The estimate scales the
whole time by ten, since the sweep interval and the lease timings set
nearly all of it (the work of a delivery is a few milliseconds and does
not scale); it was not measured at the defaults.

- **Recovery is capped by the sweep, not by the work.** Each node
  delivers at most 256 alarms per sweep (`SWEEP_DELIVER` in
  [`alarm_delivery.rs`](../crates/wasmkeep-server/src/actor/alarm_delivery.rs)),
  split between the live nodes by a hash of the address. 1,000 alarms
  take two sweeps after the 1.5 s wait, as two nodes at 256 each predict.
- **Fixed: past three overdue periods, the survivors delivered the same
  alarms.** An entry overdue three times longer than a lease period plus
  grace (4.5 s here, 45 s by default) is anyone's, so that a node nobody
  else sees can still be covered. The sweep used to walk first come, first
  served: both survivors' cursors sat on the same key and delivered the
  same 256 a sweep, so 10,000 took 38.2 s (the model: 1.5 s wait, three
  sharded sweeps at 512, then 33 sweeps of 256 for the remaining 8,464:
  37.5 s), one node's rate however many survive. Now each node delivers
  its own shard first and keeps an eighth of the cap (32) for anyone's
  entries ([`plan_sweep`](../crates/wasmkeep-cluster/src/alarm_index.rs)):
  224 own a sweep each, 448 for two, plus whichever reserved picks the
  other node has not reached. The model: 1.5 s, three sweeps at 512,
  then 8,464 / 448 = 19 sweeps: 23.5 s, against 23.2 s measured. Two
  nodes at the full 256 each would take about 21 s; the reserve, which
  keeps anyone's entries moving when a node's own entries never leave the
  index, costs the difference. At the defaults that is about 45 alarms a
  second for two survivors, and recovery now grows with the node count.
  `sim/tests/madsim_alarm_sweep.rs` checks the rate and that every entry
  is still delivered when views disagree or a live node never delivers
  its shard.
- No alarm ran twice in these runs; 14 alarm events were refused by
  admission and retried, over all samples (25 in the `9c495b4` run).

### Not measured

A real bucket at this scale (each delivery restores an actor: listings
and GETs with real round trips; the sweep lists the alarm index in pages
of 1,000; a handful of alarms at the default timings, on R2, is
[measured](#three-nodes-against-a-real-bucket)), an
SSD or slower disk, peak RSS, a cold page cache after a reboot, a node
restarting in a cluster (it also waits for a heartbeat and writes each
actor's ownership record on first use), more than 100,000 actors on a node or
10,000 alarms at once, recovery at the default timings, and alarms whose
handler fails or re-arms itself.

## Workflow replay against recorded steps

Every event that moves a workflow forward replays its recorded steps from
the top ([ADR 0019](adr/0019-workflows-as-an-sdk-pattern.md)), so its cost
grows with them, against the event's 50 ms CPU budget.
`crates/wasmkeep-server/benches/workflow.rs` times one run of an instance
with `n` recorded steps (each returning an integer), parked on a sleep:
the runner loads the steps, replays them, finds the park and sets the
alarm. It is reached through an `actors.call` from a driver actor, so
`n = 0` is the floor (HTTP, the call, the runner's own reads). From
[`bench/results/2026-11-06-30e68a8.csv`](../bench/results/2026-11-06-30e68a8.csv)
(commit `30e68a8`, the AMD Ryzen 9 7950X workstation, a Rust guest, one
node, tmpfs).

| Recorded steps | One run (median) | Over the floor |
| --- | --- | --- |
| 0 | 0.21 ms | |
| 10 | 0.24 ms | 0.03 ms |
| 100 | 0.37 ms | 0.16 ms |
| 1,000 | 1.72 ms | 1.51 ms |
| 5,000 | 7.74 ms | 7.53 ms |

### How to read it

- **About 1.5 µs a recorded step**, steady from 100 to 5,000 steps.
  A workflow with thousands of steps replays in a few milliseconds of
  wall clock, well inside the 50 ms CPU budget (which counts the guest's
  share of that time only).
- **Recording is not measured here**: the first run of each size recorded
  all its steps in one event, a durable write each.
- **A Rust guest.** TypeScript replays through JSON and an interpreter
  (QuickJS is 1.6× slower than StarlingMonkey on plain JavaScript); not
  measured yet.
- Over a workflow's life replay is O(n²): 5,000 steps that each wake the
  workflow once replay about 12.5 million positions in all.

## Three nodes against a real bucket

[`crates/wasmkeep-cli/tests/soak.rs`](../crates/wasmkeep-cli/tests/soak.rs),
run by `bench/soak.sh`: three `wasmkeep serve` nodes with the default lease
timings, eight clients making durable writes (`exec-durable`) to 60
actors through random nodes for 30 minutes, and every 3 minutes one node
killed with `kill -9`, kept down 75 s, then restarted (9 kills). Before
each kill, alarms are set on 8 of its actors, due at the moment it dies.
Each of its other actors then gets a client of its own, retrying a write
every 100 ms until one is acknowledged.
Two more clients write with a plain `exec` (answered before its upload)
to 20 actors of their own: the acknowledged increments lost are reported
(`plain_lost`: values handed out again, as later writes re-cover a lost
one, plus what the actors no longer hold at the end), not judged, since a
plain write may be lost with its node. For 30 s after each kill, four
readers read the killed node's actors through the live nodes every
100 ms (`takeover_reads_*`: answers, failures, latency).

A run fails if an acknowledged write is lost (at the end, or in a read
during a takeover), a value is handed out twice,
an alarm never fires, or the 99th percentile of "writable again after the
kill" passes 30 s (`WASMKEEP_SOAK_FAILOVER_P99_SECS`; the worst is
reported, not judged; the runs below were judged by their worst).
`WASMKEEP_SOAK_ACTORS` changes the number of actors. Each run keeps
`samples.csv`, one row per write, probe, alarm, kill and restart, which
`bench/soak_samples.py` turns into a line per kill, and every node's
metrics page at each kill.

Three runs of the same commit (`0f2986f`: per-actor leases, before
[ADR 0020](adr/0020-one-lease-per-node.md); the node-lease runs follow
below):

- **R2 from a desktop:** the three nodes as processes on the workstation
  above, over a residential connection, to an R2 bucket in
  Eastern North America.
- **OVH from a desktop:** the same, to OVHcloud Object Storage in
  Beauharnois (Standard, One Zone).
- **OVH from three VPS:** one node per OVH VPS-2 (4 vCPU, 8 GB, Debian 13)
  in Beauharnois, next to the bucket, installed as in
  [Three nodes](deploy/three-nodes.md) (systemd, `--app`, mutual TLS
  between nodes); the clients run on the first VPS. The deployment to
  expect.

| | R2, desktop | OVH, desktop | OVH, three VPS |
| --- | --- | --- | --- |
| Acknowledged durable writes lost | **0** | **0** | **0** |
| Values handed out twice (two live owners would) | **0** | **0** | **0** |
| Alarms delivered | **72 of 72** | **72 of 72** | **72 of 72** |
| Writes acknowledged in 30 minutes | 21,260 | 129,577 | 171,050 |
| Durable write, as a client sees it (p50 / p99) | 587 ms / 2.0 s | 81 ms / 0.49 s | **54 ms / 0.42 s** |
| Writable again after its node died (p50 / max) | 21.2 / 29.1 s | 16.6 / 25.9 s | 16.6 / **32.0 s** |
| Alarm, after its node died (p50 / max) | 30.2 / 33.6 s | 21.2 / 33.8 s | 19.7 / 25.1 s |
| Restarted node ready | 3.0 s at most | 2.8 s at most | 7.2 s at most |

From [`…-0f2986f-soak-r2.csv`](../bench/results/2026-11-07-0f2986f-soak-r2.csv),
[`…-0f2986f-soak-ovh-bhs.csv`](../bench/results/2026-11-07-0f2986f-soak-ovh-bhs.csv) and
[`…-0f2986f-soak-ovh-bhs-3vps.csv`](../bench/results/2026-11-07-0f2986f-soak-ovh-bhs-3vps.csv)
(the VPS run failed the soak's 30 s check, see below).

### How to read it

- **Nothing lost or duplicated in any run**, over 27 kills under load.
  Both stores enforce the conditional writes leases and replication rely
  on. One difference from moto: R2 returns a version id with a write and
  none with a read, so the two tokens for one object differ. WasmKeep only
  hands them back to the bucket, never compares them; the test that did
  is fixed.
- **A durable write costs one conditional PUT**, so its latency is the
  bucket's: 54 ms next to the bucket, 81 ms from the desktop to the same
  store, 587 ms from the desktop to R2. The long
  tails (seconds) are writes that reached an actor while it was being
  taken over.
- **Failover is the lease, then a restore.** Takeovers start about 12 s
  after the kill (10 s ttl + 5 s grace, from the last renewal). The new
  owner then downloads the actor's history one object at a time: a
  snapshot and up to 64 segments, 82 ms each at the median from the VPS
  (237 ms at p90), so most restores take 1 to 6 s.
- **One actor in 267 missed the 30 s target on the VPS (32.0 s).** Its
  restore took 19.8 s, while 21 other restores of similar size on the
  same node at the same time took 2 to 6 s: one GET hung. The bucket
  client keeps `object_store`'s defaults (30 s per request, then a
  retry), and a restore waits for each object in turn, so one slow
  request held the whole restore. Since then a restore gives up a read
  after 5 s of silence and sends it again (ADR 0007, restore step 5).
  With it, the same soak on the VPS passed: writable again 15.5 s after
  the kill at the median, 24.6 s at worst, over 269 actors; durable
  write p50 49 ms, worst 8.1 s; nothing lost or duplicated, 72 of 72
  alarms
  ([`…-c3d50ba-soak-ovh-bhs-3vps.csv`](../bench/results/2026-11-07-c3d50ba-soak-ovh-bhs-3vps.csv)).
  That run logged no read given up three times, and logged nothing for
  single retries then, so it does not show whether a stall was cut or
  none happened; every read given up is logged since.
- **Twelve hours on the VPS** (the same three nodes, a kill every 20
  minutes, commit `a72fbac`: per-actor leases, before ADR 0020): nothing
  lost or duplicated over 5,024,032 acknowledged writes, 35 kills and
  1,129 actors taken over; 280 alarms fired, none missed. Durable write
  47 ms at the median, 375 ms at p99, 18.8 s at worst (a write reaching
  an actor being taken over). Writable again after its node died: 17.0 s
  at the median, **31.6 s at worst**, so the run failed the soak's 30 s
  check once in 1,129. The nodes' logs show why: after one kill, a
  surviving node took over 19 actors within 3 s (16 s after the
  kill, the lease and its grace from the last renewal), and restored them
  in parallel; one of them, 63 segments, took 17.5 s, while another of 63
  on the same node at the same moment took 4.2 s. No read stalled long
  enough to be cut and sent again (5 s of silence), so it was slow reads,
  not a hung one; reads are not timed one by one. Alarms after their node died: 22.5 s at the median, 34.1 s
  at worst; a restarted node was ready within 8.6 s
  ([`…-a72fbac-soak-ovh-bhs-3vps-12h.csv`](../bench/results/2026-11-07-a72fbac-soak-ovh-bhs-3vps-12h.csv)).
- **Twelve hours on the VPS, on node leases** (the same nodes, bucket and
  cadence, the next night, commit `82dab61`: ADR 0020, before the bounded
  segment uploads): nothing lost or duplicated over 3,659,687
  acknowledged writes, 35 kills and 1,072 actors taken over; 279 alarms
  fired, none missed. Writable again after its node died: 18.2 s at the
  median, 24.4 s at p99 (the soak's judged bound is 30 s), **89.6 s at
  worst**, once: that actor's restore (a listing and 55 reads, one after
  another) took 56 s, each read slow but never silent for 5 s, so none was
  cut; the 11 requests whose clients had given up meanwhile then ran,
  16 s more. Nothing bounds a restore's total time yet. Alarms after their
  node died: 24.0 s at the median, 35.9 s at worst; a restarted node was
  ready within 10.4 s. Durable write 57 ms at the median, 674 ms at p99,
  29.4 s at worst. The writers wait for each answer, so the 27 % fewer
  writes than the night before are slower writes (a mean of 93 ms
  against 69 ms), most of it inside the owner's durable write; the
  per-actor runs all ran one night and these the next, so the bucket's
  speed and the design are not told apart here. Renewals are gone: PUTs
  other than segments fell from 3.1 to 7.4 per node and second to 0.80
  (node lease writes, 0.33), 1.03 PUTs per durable write against 1.17
  ([`…-82dab617-soak-ovh-bhs-3vps-12h-nodelease.csv`](../bench/results/2026-11-08-82dab617-soak-ovh-bhs-3vps-12h-nodelease.csv)).
- **Both designs on the same night** (the same three VPS and bucket):
  four 30-minute blocks, a kill every 3
  minutes, alternating node leases (`82dab61`) and per-actor leases
  (`a72fbac`, its own bucket prefix and data directories), the harness
  of `82dab61` for all four. Nothing lost or duplicated, every alarm
  fired, in all four.

  | Block | Leases | Writes | Durable p50 / p99 | Writable again p50 / p99 / worst |
  | --- | --- | --- | --- | --- |
  | 1 | per node | 144,727 | 53 / 666 ms | 17.4 / 23.6 / 34.3 s |
  | 2 | per actor | 185,012 | 50 / 419 ms | 15.5 / 19.1 / 25.4 s |
  | 3 | per node | 126,974 | 62 / 747 ms | 16.7 / 26.3 / 37.6 s |
  | 4 | per actor | 186,427 | 50 / 367 ms | 15.6 / 21.4 / 26.2 s |

  On one night, node leases made 27 % fewer writes (the writers wait for
  each answer, so that is slower writes, mostly in the tail: p99 about
  1.7 times), and actors were writable again 1 to 2 s later at the
  median. So the 12-hour runs' gap is mostly the design, not the night.
  Both node-lease blocks ran `82dab61`, before the bounded uploads and
  restore fixes; where in the durable write the time goes is not yet
  measured
  ([`…-ab1-nodelease.csv`](../bench/results/2026-11-08-82dab617-soak-ovh-bhs-3vps-ab1-nodelease.csv),
  [`…-ab2-peractor.csv`](../bench/results/2026-11-08-82dab617-soak-ovh-bhs-3vps-ab2-peractor.csv),
  [`…-ab3-nodelease.csv`](../bench/results/2026-11-08-82dab617-soak-ovh-bhs-3vps-ab3-nodelease.csv),
  [`…-ab4-peractor.csv`](../bench/results/2026-11-08-82dab617-soak-ovh-bhs-3vps-ab4-peractor.csv);
  the file names carry the harness's commit).
- **Alarms after their node died:** another node may deliver an alarm
  once it is overdue by the lease plus grace (15 s); then comes the next
  sweep (every 10 s), a restore and the deliveries in turn: 20 to 30 s at
  the median.
- **Lease renewals were most of the requests** (per-actor leases, before
  ADR 0020, which removed them). Each actor in memory renewed
  its lease 3.3 s (a third of the ttl) after its previous renewal
  returned. In the R2 run, Cloudflare counted 47,083 PUT, 4,432 LIST and
  47,762 GET for the bucket
  ([`…-soak-r2-requests.csv`](../bench/results/2026-11-07-0f2986f-soak-r2-requests.csv)):
  about 23,000 of the PUTs were renewals, more than the 21,260 writes.
  On R2 that is about $0.23 of Class A requests beyond the free tier per
  30 minutes, and a cluster's bill grew with how long actors stay in
  memory, not with what they do. OVHcloud does not bill requests.
  [ADR 0020](adr/0020-one-lease-per-node.md) replaced them with one lease
  per node; request counts since are not measured.

## The allocator: system against mimalloc

The same suite on the same workstation, twice in a row: `main` as it is
(commit `6063067`, glibc's allocator) and with mimalloc as the server's
global allocator (commit `59f01e8`, an experiment that was not adopted).
From [`2026-11-07-6063067.csv`](../bench/results/2026-11-07-6063067.csv)
and [`2026-11-07-59f01e8.csv`](../bench/results/2026-11-07-59f01e8.csv).

| Server CPU per request (8 clients) | System | mimalloc |
| --- | --- | --- |
| Rust | 102 µs | 99 µs |
| Zig | 105 µs | 102 µs |
| C | 105 µs | 102 µs |
| TypeScript (QuickJS) | 228 µs | 224 µs |
| Python | 260 µs | 254 µs |

- **No change worth a dependency.** CPU per request moved 1 to 3%, and
  throughput 1 to 2%: within what one run of this suite varies on its
  own. Memory per resident actor did not move; the server's total
  resident memory grew 4 to 7%.
- **Latency moves both ways between two runs of the same code.** The
  Rust counter's median write was 77.5 µs in the first run and 67.4 µs
  in the second, its median read 59.5 µs and 67.8 µs: a single run's
  latency rows differ by up to about 15% without any change.
- **What it means for packaging**: a static `musl` build would need a
  faster allocator than musl's own to match glibc, and mimalloc brings
  nothing measurable on glibc; a glibc build linked against an older
  glibc stays the simpler way to one portable binary. Measured below
  ([Portable builds](#portable-builds)): it is.

## Portable builds

One commit (`529fd96`) built four ways, each run as the Rust counter's
server by `bench/compare`, interleaved, three rounds; medians, from
[`2026-11-07-529fd96-portable-builds.csv`](../bench/results/2026-11-07-529fd96-portable-builds.csv).
Native is `cargo build --release` on the workstation (it needs glibc
2.39); glibc 2.28 and musl come from
[`scripts/build-portable.sh`](../scripts/build-portable.sh).

| Rust counter | Native | glibc 2.28 | musl | musl + mimalloc |
| --- | --- | --- | --- | --- |
| Server CPU per request | 104 µs | 104 µs | 243 µs | 118 µs |
| Throughput (8 clients) | 61,300/s | 61,400/s | 27,400/s | 53,000/s |
| Startup to first response | 51 ms | 51 ms | 102 ms | 52 ms |
| Memory per resident actor | 249 KB | 250 KB | 265 KB | 264 KB |

- **The glibc 2.28 build is as fast as the native one**, on every row
  within the rounds' spread, and runs on any x86_64 Linux with glibc 2.28
  or newer (RHEL 8, Debian 10, Ubuntu 20.04 and later). It is the
  candidate for release binaries.
- **A static musl build is slower**: 2.3 times the CPU per request, under
  half the throughput, twice the startup. musl's own allocator is most of
  it: with mimalloc the gap narrows to 14% more CPU per request and 13%
  less throughput, with 8% more resident memory, still behind glibc.
- The musl + mimalloc row is from a second set of rounds against native
  again (104 µs, 61,200/s there). The workstation had other work running
  (load average 7 to 14); the builds were interleaved, so it weighs on
  all of them alike.

## WasmKeep on its own

| | Median |
| --- | --- |
| Instantiate a component (store + instance) | 2.4 µs |
| Instantiate the hello component (without calling its handler) | 19 µs |
| HTTP overhead over raw hyper (stateless hello) | 108 µs |
| Request to a resident actor (counter GET) | 233 µs |
| Wake a hibernated actor (reopen SQLite + instantiate) | 1.39 ms |
| First request to a new actor (create database + `on-init`) | 3.5 ms |
| Write, single node | 243 µs |
| Write, replicated (in-memory bucket) | 0.87 ms |
| Durable write, `exec-durable` (in-memory bucket) | 1.67 ms |
| Failover: owner killed with `kill -9`, another node serves (3 runs, previous file, per-actor leases before ADR 0020) | 14.0, 14.2, 14.0 s |

A real bucket adds its PUT round trip to durable writes: 54 ms next to
OVHcloud Object Storage, 587 ms from a desktop to R2
([measured](#three-nodes-against-a-real-bucket)).
Failover is set by the node lease timings (15 to about 21 s after the
node's last heartbeat) plus a restore; see
[Running a cluster](cluster.md#how-it-works).

## Reproduce

Both entry points require Bash. Full comparisons and `bench/run.sh` require
a soft open-file limit of at least 4096; `compare --quick` requires 1024
(or unlimited). They stop before building,
clearing previous Criterion output or starting a server if the limit is
too low. The floor is the smallest power of two at which the server keeps
every actor of the workload resident: the server sizes its resident-actor
cap from its limit, 256 descriptors reserved, the harness's
`--max-connections 64`, and three per actor (`256 + 64 + 3 * (actors + 2)`).
Below it the benchmark would measure evictions. Full runs create 1,000
actors; quick runs create 100.
Check `ulimit -Sn` and `ulimit -Hn`, then explicitly
raise only the soft limit in a subshell if the hard limit allows it:

```bash
(ulimit -Sn 65536 && ./bench/run.sh)
```

The benchmark harnesses leave their inherited limits unchanged. On Unix,
`wasmkeep serve` separately attempts to raise its own soft limit within the
inherited hard limit, then sizes its connection and resident-actor caps from
the effective limit. This does not replace the harness preflight.
Choose `TMPDIR` deliberately:
RAM-backed storage and disk have very different creation and write costs.
Record the filesystem type with results, using relative paths or placeholders
for local directories.
`bench/run.sh` records the kernel's transparent huge page setting
(`/sys/kernel/mm/transparent_hugepage/enabled`) in a last `thp` column,
`unknown` where there is none. Earlier files (up to `351ea57`) lack it, so
check the machine by hand for those. Since commit `351ea57`
the server turns them off for itself, so its numbers no longer depend on
it, but runs before that on a kernel set to `always` read about twice the
memory per actor (the workstation files `cd48f41` to `dec1a82`). Do not
compare memory across that boundary.

```bash
./bench/run.sh                     # criterion suites + the comparison (if npm is installed)
WASMKEEP_TEST_S3_BUCKET=... ./bench/run.sh   # also failover, with AWS_* pointing at an S3 API
target/release/compare --quick wasmkeep-rust workerd   # just the comparison, smaller
```

Before publishing evidence, replace personal directory paths and generated
scratch paths with repository-relative paths, `[repo]` or `[scratch]`. Stage
only the intended artifacts, then run `python3 -I scripts/check-benchmark-paths.py`.
It checks the staged Git blobs under all of `bench/` and `docs/`,
so redacting only the working copy cannot conceal an unsafe staged version.
Reviewed system paths, product routes and documentation-site links are allowed;
test files are scanned too. The check recognizes named-home paths and paths
wrapped in Markdown emphasis, validates IPv6 CIDRs, and limits regex/printf
escape exceptions to complete quoted patterns or reviewed printf examples. Adjacent private paths still
fail. An [optional pre-commit hook](../CONTRIBUTING.md#pull-requests)
runs this check locally. CI checks files added or changed by every introduced
commit with `--range BASE..HEAD`, including paths removed before the final commit.
Unchanged files inherited from an old base are not attributed to the PR.
CI runs after publication, so it cannot prevent the initial push. This checks local paths; review logs
for other private information before staging them.
