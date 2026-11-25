# Benchmark archive

Runs moved here from [Benchmarks](benchmarks.md), unchanged. The rule: a
run of a version before 0.7.0 goes here once a later run of the same
measurement is on the main page, or when it compared two releases that
are both older than 0.7.0. A run that is still the latest of its
measurement stays on the main page, whatever version it ran on. The
result files stay where they were, under `bench/results/`.


<a id="a-big-room-on-three-nodes-preview"></a>
## A big room on three nodes, 0.6.0

One [ts-big-room](../examples/ts-big-room/README.md) lobby on 0.6.0
(`cac4826`) at the default 50 ms CPU budget, on three 4-vCPU nodes with a
real bucket, two apps per process. Six generators of the
[harness](benchmarks.md#reconnecting-room-harness), two per node, each joined clients
over 600 s, held 900 s (with 17 stalled readers for 60 s), then left. Six
closed-loop publishers offered 10 messages/s of 256 bytes in total. The
steps ran back to back on one room, 10,000 first, so later steps carry the
earlier history. Every generator was valid at every step.
[Results](../bench/results/2026-11-19-cac4826-room-group-ovh-bhs-3vps-r4-default-budget.json).

During the 900 s hold:

| Clients | Ready | Delivery p50 | Delivery p99 | Publishes acknowledged/s | Unacknowledged after 30 s (run) | Incomplete clients | Traps | Room node memory |
|---|---|---|---|---|---|---|---|---|
| 10,000 | 9,934 | 2.1-2.3 s | 10.2-15.4 s | 2.35 | 161 | 0 | 12, room actor | 1.75-2.44 GB |
| 6,000 | 6,000 | 1.7-1.8 s | 10.7-11.2 s | 2.63 | 40 | 0 | 0 | 2.25-2.56 GB |
| 4,500 | 4,500 | 1.3 s | 5.0-5.6 s | 4.25 | 61 | 0 | 0 | 2.39-2.50 GB |
| 3,000 | 2,977 | 0.9-1.0 s | 8.0-11.7 s | 3.78 | 11 | 1 | 4, front | 2.39-2.46 GB |

Ranges are across the six generators. The other two nodes stayed under
310 MB.

### How to read it

- **Big rooms were a preview in 0.6.0, with no supported client count** (0.7.0 changed that: see [below](benchmarks.md#a-big-room-on-three-nodes-070)).
  No step met the targets set before the run: a delivery p99 of 1 s and
  all 10 messages/s acknowledged.
- **The room no longer wedges at the default budget.** One client at
  3,000 ended without every message; none did at the larger steps.
- **The publish path is the limit, not the sockets or the edges.** A
  publish took 1 to 2.3 s to be acknowledged; the publishers wait for each
  acknowledgement, so the rate and delivery latency follow it.
- **The room's node kept its memory** after the 10,000 step and never gave
  it back.
- After the 10,000 step's leave, the room served nothing for 60 s at 2,000
  clients, then served again.
- Not measured: a killed edge node, a fresh room per step, bucket requests.

These are the first items of `0.7.0` in the [roadmap](../ROADMAP.md).

## 0.6.0 against v0.5.1

The [regression check](benchmarks.md#regression-check-on-pull-requests) run by hand
on one OVH VPS-2 (4 vCPU, Haswell): `14f7ea2` against the `v0.5.1` tag,
three interleaved rounds of the `instantiate`, `http` and `actors`
suites. From
[`bench/results/2026-11-18-14f7ea2-bench-ab-v0.5.1-3rounds.csv`](../bench/results/2026-11-18-14f7ea2-bench-ab-v0.5.1-3rounds.csv):
the median over the rounds of each round's ratio, as a change in time
(positive: `14f7ea2` takes longer).

| Benchmark | Change |
| --- | --- |
| `actor/create` (first request to a new actor) | −1.9 % |
| `actor/wake` (wake a hibernated actor) | +5.5 % |
| `actor/warm` (request to a resident actor) | +8.6 % |
| `hello/instantiate` | +5.7 % |
| `hello/request/wasmkeep` | +4.7 % |
| `hello/request/raw-hyper` (plain hyper, no WasmKeep code) | −4.0 % |
| `instantiate/store+instance` | +1.4 % |
| `instantiate/store+instance+call` | +0.2 % |

### How to read it

- **No regression past the check's threshold.** Every row is within
  ±9 %, far from the 20 % the check fails on.
- **The control moved 4 %.** Plain hyper, which runs no WasmKeep code,
  came out 4.0 % faster, so the machine drifted by about that much between
  sides. Most rows are within a few percent of it.
- **The largest change is a request to a resident actor, +8.6 %**, then
  the hello component's instantiation (+5.7 %) and a wake (+5.5 %). Three
  rounds on a shared VPS catch large regressions, not changes of this
  size; they are worth watching, not shown to be real.
