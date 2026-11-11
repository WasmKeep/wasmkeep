# 0008. `wasmkeep deploy` and rolling upgrades through the bucket

- Status: accepted (implemented 2026-11-02; the mixed-version test refuses old code, and removing the check fails it)
- Date: 2026-11-02
- Milestone: [M2](../../ROADMAP.md#m2-cluster-and-durability)

## Context

The last M2 item on the [roadmap](../../ROADMAP.md): `wasmkeep deploy`
uploads artifacts and manifests, and nodes roll to the new version without
dropping actors. Today each
node is started with `--actor CLASS=PATH` and runs whatever files it was
given. A cluster needs one source of truth for which code runs, with no
control plane besides the bucket (a design rule, see the
[overview](../overview.md): if the bucket is up, WasmKeep is up).

What already exists:
- `ActorService::replace_class` swaps a class at the next event boundary.
  Running events finish on the old code, databases and sockets are kept, and
  `on-init` runs once per component version.
- The component cache compiles each artifact once per node.
- Leases make an actor run on one node at a time.

The new hazard is **mixed versions during a rollout**. Nodes pick up a new
version at slightly different moments. Suppose an actor runs `on-init` for
version 7 on node A (migrating its schema), then moves to node B, which still
runs version 6. B would run old code on a migrated database.

## Decision

### Bucket layout

```text
apps/<tenant>/<app>/<version>/<class>.wasm   as uploaded, immutable
apps/<tenant>/<app>/<version>/manifest       "wasmkeep-manifest 1", previous <version>, class <name> <sha256>
apps/<tenant>/<app>/current                  {version, seq}; replaced with If-Match only
```

The manifest is a few lines of text (no JSON dependency); `seq` lives in
`current`. `version` is the first 16 hex digits of the SHA-256 of the class table, so a redeploy of the
same code is a no-op. `seq` is a counter that increases with every change of
`current`, including rollbacks.

### `wasmkeep deploy`

1. Compile every component locally and link it against the actor world, with
   the same check `serve` runs. A broken component is refused before
   anything is uploaded.
2. Upload the artifacts and the manifest with conditional create. They are
   immutable; an existing object with the same key is the same content.
3. Read `current`, then replace it with `{version, seq: old + 1}`, using
   `If-Match` on the version read. A concurrent deploy fails loudly instead
   of silently overwriting.

`wasmkeep deploy --rollback` re-points `current` to the manifest's
`previous`, with a new, higher `seq`. Code never goes back in `seq`, even
when it goes back in content.

### Nodes

`wasmkeep serve --bucket ... --app <tenant>/<app>` replaces `--actor`. The
node reads `current` at start and with every heartbeat (3 s). When it
changes, the node:
1. downloads and compiles all the new classes;
2. swaps them in, and the `seq` with them, at once (`apply_deploy`).

A node that cannot build a version keeps the previous one, whole, and
reports the error in its heartbeat.

### No old code on newer data

Each actor's database records, in `_wasmkeep_meta`, the highest deploy `seq`
that has run an event on it. A node never runs an event on an actor whose
recorded `seq` is above its own. Instead it re-reads `current` at once and
upgrades first. If it still cannot (the new version fails to build on it),
it refuses with 503 and Retry-After, so the actor is never downgraded. The
check costs one meta read per activation, not per event.

Rollback stays safe: rolling back produces a higher `seq`, so it is allowed
everywhere. Schema compatibility on rollback is the application's job, as
it is for any schema migration.

## Alternatives

- **Push to nodes** (deploy calls every node). It needs node discovery and
  reachability from the deploy machine, and it fails partially. Polling the
  bucket fits "the bucket is the only shared state".
- **A version per actor in the lease record.** That couples deploys to the
  lease protocol, which is proven and should stay small. The meta row lives
  with the data it protects and is replicated with it.

## Consequences

- A rollout takes about one heartbeat interval (3 s) to reach every node.
  Actors switch at their next event, and none is dropped.
- New CLI command, new `serve` mode, a manifest format and a test: two nodes,
  deploy v1, write, deploy v2 with an `on-init` migration, move the actor to
  the node that hasn't polled yet, and assert it upgrades first or refuses
  rather than running v1.
- Artifact garbage collection (old versions) joins the bucket GC work.

## Implementation notes (2026-11-02)

- Classes dropped by a new version (or a rollback) stop being served: new
  requests get 404, and their actors' databases are kept.
- A node that refuses an actor answers with an error and wakes its
  follower at once; in the test the node caught up and served within a
  second, instead of waiting for its next poll.

## Revision (2026-11-04): the sequence travels with the code

The node's sequence was read when an actor recorded it, separately from the
class the event had taken. Classes were replaced one by one, then the
sequence raised. That left three ways to break "no old code on newer data",
found from a 1-in-80 test flake under load and an adversarial review:

- **Old code under the new sequence.** An event took the v1 class, then
  waited (database open, instantiation); deploy 2 landed meanwhile; the
  event read sequence 2 and recorded it, running v1.
- **New code under the old sequence.** An event between "class replaced"
  and "sequence raised" ran v2 and recorded 1. A resident instance never
  re-recorded, so a node still on deploy 1 could later take the actor
  over and run v1 on data v2 had written.
- **Half a deploy.** A class that failed to build left the ones before it
  replaced and the sequence not raised, indefinitely (retried, and torn
  down, every poll).

Now each class carries the sequence it was swapped in under. An event takes
both together, and records the class's sequence, not the node's. A deploy
is compiled whole, then swapped in with its sequence under one lock. An
instance records again whenever its class's sequence moves past what it
recorded (the same bytes redeployed keep their instances). A class no
longer served refuses events, including for actors already resident.
Tests: `crates/wasmkeep-server/tests/deploys.rs`, mutation-checked; a storage fault-injection delay
holds an actor's start-up while a deploy lands.

A second review round added: classes are stamped with their sequence when
the node starts (`ActorConfig::deploy_seq`), so nothing runs unchecked
before the follower's first update; a class removed by a deploy closes its
resident actors' sockets (1012) and drops their alarms from the timer
(they stay in the database and the bucket's index); the follower backs off
after a failed update (up to 5 minutes, wake-ups included) instead of
recompiling every poll. A node serving `--actor` classes (sequence 0) does
not check sequences at all: do not point one at a bucket that deployed
apps use.

## Revision (2026-11-05): rules, configuration and lineage

[ADR 0013](0013-access-rules-and-config-in-deploys.md) adds access rules
and plain configuration to the version (manifest format 2), puts
`previous` into the version id (the class table alone made a directory
reused under a stale `previous`), and adds a sticky third field to
`current` that tools and nodes from before refuse.
