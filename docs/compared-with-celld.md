# WasmKeep and celld

[celld](https://github.com/denoland/celld) and WasmKeep solve the same
problem the same way: Durable Object-style actors on your own servers,
each with its own SQLite database, replicated to an S3-compatible bucket,
and taken over by another node when one dies. This page says where each
is the better choice. Numbers come from [Benchmarks](benchmarks.md), where
both run the same counter on the same machine and the same bucket: one
node ([measured](benchmarks.md#wasmkeep-against-celld-every-write-in-a-bucket))
and three nodes with a node killed under writes
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
**not measured yet**: on the local bucket used so far, the bucket answered
faster than a follower's disk. Against a write that waits for the bucket
in both, the numbers below compare like with like.

In both, no acknowledged write was lost: three kills, 270 actors written
every 20 ms, every runtime and mode, every value handed out once.

## Where WasmKeep is the better choice

- **Any language, as an equal.** Actors are standard WebAssembly components
  run by Wasmtime: Rust, TypeScript, Python, C and Zig, with Go (TinyGo)
  experimental ([languages](languages.md)). celld runs the Workers API on
  V8: JavaScript first, other languages as WebAssembly inside it.
- **Less CPU and memory on one node.** On the same counter, each write in
  the bucket, WasmKeep with Rust used 85% of celld's CPU per request and
  answered a write in 0.55 against 0.97 ms at the median, and reads twice as
  fast ([measured](benchmarks.md#wasmkeep-against-celld-every-write-in-a-bucket)).
- **Faster reads and less memory on three nodes.** Reads took 96 against
  221 µs at the median, and three nodes held 1000 actors in 441 against
  516 MB ([measured](benchmarks.md#three-nodes-wasmkeep-against-celld)).
- **Encrypted traffic between nodes.** WasmKeep nodes talk mutual TLS by
  default ([cluster](cluster.md)); celld has no TLS between peers, so its
  peer listener belongs on a private network.
- **A write that does not wait.** For data that can be rebuilt, a plain
  `exec` answers before its upload: 0.56 ms at the median on three nodes,
  and 13,205 writes per second against 1,679 durable.

## Where celld is the better choice

- **Existing Workers code.** celld runs the Workers API and Durable
  Objects as they are: the same counter ran on workerd and celld unchanged.
  WasmKeep has its own API, and Workers compatibility is not planned
  ([roadmap](https://github.com/WasmKeep/wasmkeep/blob/main/ROADMAP.md)).
- **Faster takeover and start.** With each one's defaults, a dead node's
  actors were writable again after 9 to 10 s on celld against 15 to 16 s
  on WasmKeep, which follow their lease timings (10 s and 15 s). A celld
  node starts in about 0.1 s; a WasmKeep cluster node checks for 15 s that
  no other process has its node id before it listens.
- **More durable writes per second on three nodes, waiting for the
  bucket.** In bucket mode, celld handled 1,889 writes per second against
  WasmKeep's 1,679, at 5% less CPU per write, although WasmKeep answered
  a single write sooner at the median (2.29 against 2.69 ms)
  ([measured](benchmarks.md#three-nodes-wasmkeep-against-celld)). One
  reason may be that a WasmKeep actor uploads one write's changes before
  it runs its next event; that is not measured yet.
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
  machine, so a follower was as close as one gets.
- **Longer runs.** WasmKeep has a twelve-hour soak against a real bucket
  ([measured](benchmarks.md#three-nodes-against-a-real-bucket)); celld was
  run for minutes.

## In short

Both keep each actor's data in SQLite, in a bucket you own, and kept every
acknowledged write through the kills measured here. WasmKeep fits
stateful apps in compiled languages, with less CPU and memory per actor
and encrypted traffic between nodes. celld fits Workers code you already
have, and starts and takes over faster with its defaults. Which answers a
durable write sooner on a cloud bucket is the open question, and the next
measurement.

celld is Deno's project, and Cloudflare Workers and Durable Objects are
Cloudflare's products, named here only to identify them
([licensing](legal.md#trademarks-of-others)).
