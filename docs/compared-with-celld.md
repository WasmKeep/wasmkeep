# WasmKeep and celld

[celld](https://github.com/denoland/celld) and WasmKeep solve the same
problem the same way: Durable Object-style actors on your own servers,
each with its own SQLite database, replicated to an S3-compatible bucket,
and taken over by another node when one dies. This page says where each
is the better choice. Numbers come from [Benchmarks](benchmarks.md), where
both run the same counter on the same machine and the same bucket: one
node ([measured](benchmarks.md#wasmkeep-against-celld-every-write-in-a-bucket))
and three nodes with a node killed under writes, five runs with the nodes'
data on disk and five on tmpfs
([measured](benchmarks.md#three-nodes-wasmkeep-against-celld)).
Everything else describes how the two are built, from their documentation.
celld v0.6.3 was measured.

## Two meanings of durable

The biggest difference is what a write waits for once there are several
nodes.

- **WasmKeep** answers a durable write (`exec-durable`) once the bucket
  holds it, on one node or many. A plain `exec` answers before its upload,
  and may be lost with its node.
- **celld** always waits. On one node, it waits for the bucket. With two or
  more nodes, by default (`fleet`), it answers once a follower node has the
  write on disk, or once the bucket has it, whichever comes first; the
  upload follows. `CELLD_DURABILITY=bucket` makes it wait for the bucket
  as WasmKeep does.

A follower's disk is usually much closer than a bucket, so celld's default
should answer writes sooner when the bucket is in the cloud. That case is
**not measured yet**. In the three-node runs with the nodes' data on one
shared disk, fleet mode was celld's slowest in every run (and it
completed fewer writes than bucket mode on tmpfs too), which is not
explained. Against a write that waits for the bucket in both,
the numbers below compare like with like.

Neither lost an acknowledged durable write. Every write carried a token
of its own, and every token was read back after each restart and at the
end: sixty kills for each of WasmKeep durable, celld fleet and celld
bucket, half of them with the killed node's disk removed before it
restarted. A control run that must lose writes lost them every time, so
the check finds a loss. What it does not test: a power loss on the nodes
that survive, and nodes on separate machines
([lost writes](benchmarks.md#lost-writes)). The figures below are medians of five runs with the nodes' data
on disk, unless tmpfs is named.

## Where WasmKeep is the better choice

- **Any language, as an equal.** Actors are standard WebAssembly components
  run by Wasmtime: Rust, TypeScript, Python, C, Zig and Go (TinyGo), with
  D and MoonBit experimental ([languages](languages.md)). celld runs the
  Workers API on V8: JavaScript first, other languages as WebAssembly inside it.
- **Less CPU and memory on one node.** On the same counter, each write in
  the bucket, WasmKeep with Rust used 85% of celld's CPU per request,
  held 1000 actors in 334 against 388 MB, answered a write in 2.31 against
  2.55 ms at the median (0.53 against 0.93 ms on tmpfs) and read about
  twice as fast. On disk its throughput was close to celld's, about 5%
  behind at the median
  ([measured](benchmarks.md#wasmkeep-against-celld-every-write-in-a-bucket)).
- **Faster reads and less memory on three nodes.** Reads took 92 against
  about 220 µs at the median, and three nodes held 1000 actors in 459
  against about 520 MB
  ([measured](benchmarks.md#three-nodes-wasmkeep-against-celld)).
- **Encrypted traffic between nodes.** WasmKeep nodes talk mutual TLS by
  default ([cluster](cluster.md)); celld has no TLS between peers, so its
  peer listener belongs on a private network.
- **A write that does not wait.** For data that can be rebuilt, a plain
  `exec` answers before its upload: 0.55 ms at the median on three nodes,
  and 13,416 writes per second against 1,710 durable. It is the one mode
  that lost writes when a node was killed: 0 to 3 of about 86,000 in a
  run.

## Where celld is the better choice

- **Existing Workers code.** celld runs the Workers API and Durable
  Objects as they are: the same counter ran on workerd and celld unchanged.
  WasmKeep has its own API, and Workers compatibility is not planned
  ([roadmap](https://github.com/WasmKeep/wasmkeep/blob/main/ROADMAP.md)).
- **Faster takeover and start.** With each one's defaults, a dead node's
  actors were writable again after 9 to 10 s on celld against 15 to 16 s
  on WasmKeep (medians, data on disk), celld sooner in every run; both are
  close to their default lease timings (10 s and 15 s). A celld
  node starts in about 0.1 s; a WasmKeep cluster node's first start checks
  for 15 s that no other process has its node id before it listens, and a
  node restarted after its peers fenced it was ready in about 1.6 s.
- **Durable writes on three nodes with data on disk, waiting for the
  bucket: close, celld ahead.** In bucket mode, eight clients completed
  1,857 writes per second on celld against 1,710 on WasmKeep, about 8%
  apart where runs differ by 2 to 4%, at the same CPU per write; WasmKeep
  answered a single write sooner at the median (2.30 against 2.68 ms) and
  later at p99
  ([measured](benchmarks.md#three-nodes-wasmkeep-against-celld)). With the
  nodes' data on tmpfs the order reverses (9,137 against 5,888 writes per
  second); the runs change only where the data is, and do not say which
  step of a write the disk slows.
- **JavaScript-heavy code.** V8 compiles JavaScript as it runs; WasmKeep's
  TypeScript runs on QuickJS, an interpreter
  ([WasmKeep and Workers](compared-with-workers.md#where-workers-is-the-better-choice)).
- **More of the platform built in.** celld comes with key-value, queues,
  D1-style SQL and R2-style object storage. WasmKeep offers key-value,
  queues and workflows as SDK patterns on actors
  ([key-value and queues](kv-and-queues.md), [workflows](workflows.md)).

## Not measured yet

- **A cloud bucket**, next to the nodes. It decides whether celld's fleet
  mode answers writes sooner than waiting for the bucket, and by how much.
- **Nodes on separate machines.** The three nodes ran as processes on one
  machine and shared its disk.
- **Longer runs.** WasmKeep has a twelve-hour soak against a real bucket
  ([measured](benchmarks.md#three-nodes-against-a-real-bucket)); celld was
  run for minutes.

## In short

Both keep each actor's data in SQLite, in a bucket you own, and no
acknowledged durable write was lost through the kills measured here.
WasmKeep fits stateful apps in compiled languages, with less memory per
actor, less CPU on one node, faster reads and encrypted traffic between
nodes. celld fits Workers code you already
have, and starts and takes over faster with its defaults. Which answers a
durable write sooner on a cloud bucket is the open question, and the next
measurement.

celld is Deno's project, and Cloudflare Workers and Durable Objects are
Cloudflare's products, named here only to identify them
([licensing](legal.md#trademarks-of-others)).
