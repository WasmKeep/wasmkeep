# 0006. Deterministic simulation tool

- Status: **accepted: madsim**, after the bake-off below (it reversed the initial proposal)
- Date: 2026-11-02
- Milestone: [M2](../../ROADMAP.md#m2-cluster-and-durability)

## Context

The project requires a deterministic simulation suite for leases, failover
and replication, built before the replication code, with seeded runs and
fault injection, 200 seeds per push and 20,000 nightly. There are two
candidates, madsim and turmoil, and the plan is to try both on a toy lease
protocol first.

What we know before trying:

| | madsim 0.2.34 | turmoil 0.7.2 |
| --- | --- | --- |
| Model | Replaces tokio, time and randomness under `--cfg madsim`; deterministic task scheduling | Simulated hosts and network on top of real tokio, one thread, seeded |
| Intrusiveness | The whole build switches runtime; crates that spawn OS threads or do real I/O (SQLite blocking threads, Wasmtime's epoch thread) break determinism | Only the code under test must use turmoil's network and time; the rest of the workspace is untouched |
| Faults | Kill and restart nodes, partitions, latency, clogging | Hold and release links, partitions, latency, crash and bounce hosts |
| Maintenance | Active (2025-10) | Active (2026-04, tokio-rs) |

Either way, the cluster logic must not touch the network, clock or bucket
directly: it is written against small traits (`Bucket` with conditional put,
a clock, a node-to-node transport), so the simulation can drive it.

## Initial proposal (superseded by the results)

turmoil, for being less intrusive than madsim's global runtime swap.

## The bake-off

1. A toy lease protocol on a `Bucket` trait (conditional create/update with
   versions, as S3 and MinIO provide), three nodes, a TTL and renewals.
2. The same protocol run under both tools with partitions, crashes, delays
   and a slow bucket, checking the invariant "never two live owners".
3. A deliberately planted bug (renewal without the version check) must be
   found by both within the per-push seed budget, and a failing seed must
   reproduce exactly.
4. Record: time per 1,000 seeds, lines of harness code, whether failures
   reproduce, and anything that broke determinism. The ADR is then accepted
   or amended with those numbers.

## Results (2026-11-02)

The same protocol code (`sim/src/lease.rs` then, now
`crates/wasmkeep-cluster/src/lease.rs`, written against `Bucket` and
`Env` traits), the same bucket semantics, the same seeded fault schedule
(links held then released for up to 2.5 s, which is the late-write case,
partitions, node crash and restart), 3 nodes, 20 s simulated per seed, debug
build, 4 vCPU container:

| | turmoil 0.7.2 | madsim 0.2.34 |
| --- | --- | --- |
| Correct protocol, 200 seeds | no double ownership; 1,461 ownership changes | no double ownership; 1,552 ownership changes |
| Planted bug (blind renew) | found at seed 1 | found at seed 1 |
| Failing seed reproduces | yes, identical history | yes, identical history |
| Wall time for 200 seeds | 127 s | 1.3 s |
| Build | plain `cargo test` | `RUSTFLAGS="--cfg madsim"`, own target dir |

Both find the bug and both are deterministic. madsim is about 100 times faster
because it jumps virtual time to the next event, where turmoil steps a fixed
1 ms tick. At the nightly budget (20,000 seeds) that is about two
minutes against several hours, and it leaves room for far more seeds.

The intrusiveness concern did not materialise: it only matters for code that
runs under the simulation, and the simulated code is protocol logic behind
traits, with no Wasmtime, SQLite or network dependencies.

Building the turmoil harness also surfaced a harness bug worth remembering:
a simulated bucket that served one connection at a time let a single held
client block everyone (turmoil reported the backlog overflow). A simulated
dependency must be as concurrent as the real one, or it hides races.

## Decision

- **madsim** for deterministic simulation; the turmoil harness was removed so
  only one is maintained.
- Cluster protocols (leases, replication ordering, fencing) are written as
  logic generic over `Bucket`, `Env` and transport traits, in modules that
  compile without tokio, hyper or object_store. Production adapters
  (object_store, tokio) implement the traits outside those modules. This is
  what lets `--cfg madsim` build them.
- CI runs the simulations with 200 seeds on every push; the nightly run uses
  20,000 (release build).
