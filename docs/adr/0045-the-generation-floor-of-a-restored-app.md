# 0045. The generation floor of a restored app

- Status: accepted (2026-11-27), built: see
  ["Built"](#built).
- Milestone: `0.10.0` (see [Now](../../ROADMAP.md#now) in ROADMAP.md)
- Follows: [ADR 0020](0020-one-lease-per-node.md) (ownership records
  under a node lease; section 6, the per-actor lease marker),
  [ADR 0028](0028-actors-that-end.md) (a deleted actor keeps its
  ownership record), [ADR 0033](0033-data-access-export-and-online-backup.md)
  (the whole-app restore), [ADR 0043](0043-rolling-upgrades.md) (the
  floor, the gate table, section 6 on removing old paths)
- Changes: one new object per app, gated; the per-actor lease marker is
  no longer written once an app's upgrade floor is `0.10`. No WIT
  change, nothing on a hop.

## Context

An actor's ownership record carries a generation. Each acquisition
writes the record one generation higher, and history objects carry the
generation of the holder that wrote them, so a late upload from an
earlier holder is never taken for the head. The rule that must hold
for ever: **an actor's next generation is above every generation its
history ever carried, deleted history included.**

When an actor has no ownership record yet, its first one is seeded. In
the code at `v0.9.0`:

- `seed_generation`
  (`crates/wasmkeep-server/src/cluster/lease_hold.rs`) lists the
  actor's history keys for the highest generation they carry
  (`highest_generation_in_bucket`,
  `crates/wasmkeep-server/src/replica/restore.rs`), then calls
  `mark_old_lease` (`crates/wasmkeep-cluster/src/node_lease.rs`),
  and returns one above the result. It is called only for an absent
  record (`Owners::acquire`).
- `mark_old_lease` reads `leases/<class>/<name>`. Absent: it creates a
  marker, `wasmkeep-lease 2` and one number (`encode_marker`), carrying
  the keys' generation. A marker: it returns the higher of the marker's
  number and the keys'. A per-actor lease from before node leases
  (`wasmkeep-lease 1`): refused while live, otherwise replaced by a
  marker carrying its generation.

So every actor's creation costs one listing, one GET and one PUT, and
leaves one object for good. The marker has had two jobs:

1. **Keeping a binary from before node leases off the actor**, and
   carrying that old lease's generation across. Such binaries are older
   than `0.4`.
2. **Carrying a restore's floor.** A whole-app restore in place reads
   every history key, ownership record and marker of the target for
   each actor's highest generation
   (`crates/wasmkeep-cli/src/backup/restore.rs`), adds the generations
   of the plans it will write, adds one to each, keeps them in its
   journal (`seeds`), deletes the app's state, and then writes one
   marker per actor, including actors absent from the run. After it,
   an actor's ownership record is gone, and its marker is the only thing
   that keeps its next generation above what the deleted history
   carried.

ADR 0043, section 6, kept the marker in `0.9.0` for the second job. A
single actor's delete does not need it: ADR 0028 keeps the ownership
record of a deleted actor, whose generation never goes down.

## Decision

### 1. One floor per app, not one per actor

The restore's floor moves to one object per app:

- **Key:** `<prefix>state/<tenant>/<app>/generation-floor`. Under the
  app's state, like the markers it replaces, so it has their life: an
  `app delete` removes it with the ownership records, and an app created
  again starts from nothing, as today.
- **Body, exactly:**
  `wasmkeep-generation-floor 1\n<generation>\nmarkers <kept|none>\n`,
  the generation a decimal number with no sign and no leading zeros,
  within 64 bits. Decoding is exact, as the upgrade floor's is
  (ADR 0043, section 3): anything else is unreadable, and an unreadable
  object fails an acquisition that needs it and every command that
  would write it, naming the key.
- **Meaning.** No actor of this app gets a first ownership record at or
  below `<generation>`. `markers none` says no per-actor marker is left
  under the app.
- **Absent** reads as generation 0 and `markers kept`.
- **Written by conditional writes only**, and the generation only
  upward; `kept` may become `none`, never the reverse.

One number for the whole app is enough because a generation is compared
only within one actor's history: `seed_generation`, the ownership
record, the restore's seed collection, the replica (one history folder
per actor) and the alarm index (entries keyed by their actor) compare
no generations of two actors. The price is that every actor first
acquired after a restore starts at the app's highest generation rather
than its own. A generation is 64 bits and grows by one per acquisition.

It is not under `formats/`: that folder survives a delete and recreate
(ADR 0043, section 3), and a generation floor that outlived the state it
protects would be harmless but would never go.

### 2. Seeding from it

`seed_generation` in a `0.10` node, at an actor's first acquisition:

1. Lists the actor's history keys, as today.
2. Unless this node has read `markers none`: reads the actor's marker.
3. **While the upgrade floor this node knows is below `0.10`: reads the
   upgrade floor again.** The floor only rises, so once a node has read
   `0.10` it keeps that and never reads it here again.
4. Still below `0.10`: `0.9`'s path, with what step 2 read. The marker
   is created, or an old lease replaced, and the seed is as today. No
   generation-floor object is read: none can have been written, its
   writers being behind the floor.
5. At `0.10`: reads the app's generation floor (never cached), writes
   nothing, and returns one above the highest of the keys, the marker
   or old lease read in step 2, and the floor. An old lease that is
   still live refuses, as today; one that is not is read and left.

Steps 3 and 4 are what let a node that was serving when the operator
finalized change path without a restart, and they are needed for more
than cost. A node that kept `0.9`'s path after a `finalize` would never
read the generation floor, and once the cleanup of section 4 has
removed a marker that was the only carrier of a restore's floor, that
node would seed the actor from its keys alone. So no `0.10` node takes
`0.9`'s path on a floor it has not just read.

The order is what makes it safe, and the cleanup is its mirror. The
node reads the marker, then the upgrade floor, then the generation
floor. The cleanup runs only above the upgrade floor, raises the
generation floor, then deletes the marker. So:

- a node that reads the upgrade floor below `0.10` in step 3 read the
  marker before any cleanup could start: what it read was not removed
  by one;
- a node at `0.10` that finds the marker gone reads a generation floor
  that already covers it.

A marker created in step 4 just after a `finalize`, by a node whose
read came just before it, is a leftover that carries nothing its
actor's keys or ownership record do not: harmless, and the cleanup or
`cluster verify` finds it.

`markers` may be cached, because it only ever goes from `kept` to
`none`: a stale `kept` costs a GET and nothing else. A node reads it
with the generation floor at each first acquisition anyway.

Cost of an actor's creation, in bucket requests:

| | Listing | GET | PUT |
|---|---|---|---|
| `0.9` | 1 | 1 (marker) | 1 (marker) |
| `0.10` before `finalize` | 1 | 2 (marker, upgrade floor) | 1 (marker) |
| `0.10` after `finalize` | 1 | 2 (marker, generation floor) | 0 |
| After the cleanup (section 4), once the node has read `none` | 1 | 1 (generation floor) | 0 |

The first creation on each node after a `finalize` costs both floors'
GETs once. `markers none` is cached in memory only, so after a restart a
node's first creation pays the marker's GET again, once. So between its upgrade and its `finalize`, a cluster pays
one GET more per created actor than `0.9` does; that is the price of
needing no restart, and a reason not to stay unfinalized.

The floor's GET could be cached once `markers none` is read, since the
floor then changes only in a restore, which runs with no node of the app
live. That is left out of `0.10`: it needs the restore's fence to
invalidate it, and one small GET per created actor is already less than
today.

### 3. The restore

When the target app's upgrade floor is `0.10` after the restore's own
floor step (ADR 0043, section 3, "A restore"):

- **Collecting**, in place: as today, over history keys, ownership
  records, markers and old leases, **and the existing generation-floor
  object**. Without that last read, a second restore in place would
  lose the first one's floor for every actor not acquired in between.
- **Writing:** one object, the highest generation collected plus one,
  instead of one marker per actor. By conditional write, after the
  history is copied, where the markers are written today. `markers` is
  `none` for a restore in place (the state was cleared, so none can be
  left) and for a target that held nothing; a resume rereads before it
  writes.
- **A restore into another prefix** writes the object too, from the
  plans' generations, where it writes markers today. Markers there
  carry the plans' generations without the one added in place; the
  object adds one always, which is never wrong.

The journal's `seeds` map stays for one release, so that a restore
started by a `0.9` `wasmkeep` and resumed by a `0.10` one finds what it
expects; the new writer uses its maximum.

When the target's upgrade floor is below `0.10`, the restore writes
markers, exactly as `0.9` does.

### 4. Removing the markers that exist

`wasmkeep cluster markers clean --bucket <url> --app <tenant>/<app>
[--yes]`. Refused while the app's upgrade floor is below `0.10`. It may
run with nodes live. Per marker:

- An ownership record for the actor at or above the marker's number:
  the record carries it. Delete the marker.
- Otherwise the marker is the only carrier. Raise the app's generation
  floor to at least the marker's number (one conditional write for the
  whole run, to the highest such number), **then** delete the marker.
- A `wasmkeep-lease 1` object that is not live: its generation is
  folded into the floor the same way, then it is deleted. One that is
  live by the old rules stops the command, naming it.

When a listing after the deletes shows `leases/` empty, the command
writes `markers none`. From then on nodes skip the marker's GET.
Without `--yes` it prints the counts and writes nothing. It is
idempotent and may be interrupted at any point: every step leaves the
rule of the Context true.

`wasmkeep cluster verify` learns the object (exact form, one per app),
keeps counting leftover markers, and reports an app whose object says
`none` while a marker exists.

`cluster upgrade finalize` does not run the cleanup: it is one more
command on the upgrading page, run once per app after the finalize.

### 5. The gate

The object is a new key under `state/`, and a `0.9` node skips unknown
keys there in silence: by ADR 0043's rule it needs a gate.

- **Row in `upgrade::FORMATS`:** `wasmkeep-generation-floor 1`, since
  `0.10`, gated on the floor at `0.10` and on a new capability word,
  `genfloor`, that every `0.10` node announces. The word covers a node
  that started while the floor was being written, as for every gate.
- **Writers:** the restore and the cleanup, both through
  `apps::Readers::gate`.
- **The marker's row** (`wasmkeep-lease 2`) stays, since `0.9` writes
  it; its rule becomes "read only from `0.10`" once the floor is `0.10`.

**Before `finalize`, a `0.10` node and a `0.10` `wasmkeep` write
exactly what `0.9` does.** `seed_generation` takes `0.9`'s path while
the upgrade floor it has just read is below `0.10` (section 2): it
writes the marker, and the one thing it adds is a read of the upgrade
floor, which a `0.9` node cannot see. The restore writes markers.
Nothing on a hop changes. So both directions of the mixed-version case
hold by construction, and this record does **not** call for a
simulation of two protocol versions. The mixed-version check gets four
lines:

- an actor created by `0.10` before `finalize` has a marker, and a
  `0.9` node takes it over;
- an actor created by `0.9` is taken over by `0.10` after `finalize`
  at a generation above its marker's;
- a restore run by `0.10` before `finalize` leaves markers and no
  generation-floor object;
- a node that served through the `finalize`, never restarted, creates
  an actor with no marker, and after a cleanup creates one above a
  removed marker's number.

Today a serving node reads the upgrade floor only at start and when it
sees a newer live node (ADR 0043, section 3). Section 2 adds a third
occasion, an actor's first acquisition while the node's floor is below
its own version. It adds nothing to an idle cluster and stops at the
`finalize`. That read is its own; it is not shared with the one a
restore's fence check makes.

### 6. What `0.10` keeps, and `0.11` drops

- `0.10.x` reads markers, because `0.9` writes them (ADR 0043,
  section 6).
- `0.11` may drop the marker's read and `mark_old_lease`: the `0.10`
  tree has no writer once its floor is `0.10`. An app whose object does
  not say `markers none` is then refused at start, with the cleanup
  command in the message; `0.11`'s record decides the exact check, and
  whether it runs the cleanup at start instead.

## Consequences

- An actor's creation writes nothing but its ownership record, and
  leaves no second object behind.
- A restore writes one small object where it wrote one per actor.
- After a restore, actors start at the app's highest generation.
- **The marker's first job ends.** A new actor has no marker, so a
  binary from before node leases pointed at the bucket would find no
  per-actor object refusing it. Such a binary is older than `0.4`,
  cannot read the layout since ADR 0022's migration, and is four minor
  versions outside what an upgrade supports.
- One more exact-form object can be unreadable. As for the upgrade
  floor, there is no repair command: it is rewritten by hand.
- Until the cleanup runs, creating an actor costs one GET more than
  today and one PUT less; between an upgrade and its `finalize`, one
  GET more and nothing less.

## Alternatives rejected

- **A floor per actor in one index object.** Exact per actor, but it
  grows with the app and every restore rewrites it.
- **Keeping the markers and writing them only in a restore.** No PUT at
  creation, but the GET stays for ever, and so does one object per
  restored actor.
- **A tombstone ownership record per actor at restore.** It reuses a
  format, but writes one object per actor again, and a record naming
  nobody is "nobody holds it" for routing: every restored actor would
  look like one that ran.
- **The floor in the restore's fence (`restored`).** Its body is frozen
  and compared byte for byte (ADR 0043's table).
- **Caching the floor on the node.** Section 2.

## Tests

- The object: exact decode, each refused form, upward only.
- `seed_generation`: each row of section 2's table by counted requests;
  a marker above the floor, a floor above the marker, both below the
  keys.
- The restore: in place, a second one in place with no acquisition
  between, into an empty prefix, resumed across versions, and below the
  upgrade floor (markers, byte for byte as `0.9`).
- The cleanup: each of the three cases, an interrupt after the floor's
  write and after each delete, a rerun, a live old lease.
- Simulation, in `madsim_node_lease.rs` and the restore model: for
  every seed, no actor is acquired at a generation its history ever
  carried, across a restore, a delete and recreate after it, a
  `finalize` with nodes serving through it, and a cleanup racing first
  acquisitions. Mutations: the cleanup deleting before it raises; the
  node reading either floor before the marker; a node that keeps the
  upgrade floor it read at start.
- The gate: the row's test, and a restore refused beside a live node
  without `genfloor`.

## Built

As decided, with these notes:

- **Where the code is.** `crates/wasmkeep-cluster/src/generation_floor.rs`
  holds the object, its exact decoding and its raise, the seed of
  section 2 (`seed`, with `mark_from_read` for `0.9`'s path, which
  `mark_old_lease` now calls) and the cleanup (`plan_clean`, `clean`).
  The server's `seed_generation` calls `seed`; the node keeps the highest
  upgrade floor it has read, set only from a read. A node whose own
  version is below `0.10` never reads the floor again there, so a build
  of `0.9` makes the requests it made before.
- **The marker's row** (`wasmkeep-lease 2`) stays `Frozen` in
  `upgrade::FORMATS`: a `0.10` node below the floor still writes it,
  byte for byte, so it is not read-only yet. The comment on the row says
  it is read only once the floor is `0.10`.
- **The gate table's test** allowed no row newer than the build's own
  minor version; it now allows the next one, so that a format of `0.10`
  can land before the release raises the workspace version. Such a
  format is written only by a build that reports `0.10`.
- **A restore beside a live node without `genfloor`.** A restore refuses
  every live node of the app before it writes, so the gate's half on the
  live nodes cannot block it there; the row's own test covers that half,
  and `cluster markers clean`, which runs with nodes live, is refused by
  it (a test).
- **The journal** keeps the target's own generation floor as
  `generation_floor`, beside `seeds` and not in it: a resume by a `0.9`
  `wasmkeep` writes one marker per name in `seeds`.
- **The simulation** is a bounded interleaving model
  (`sim/tests/support/generation_floor.rs`, run from
  `madsim_node_lease.rs` and `madsim_restore.rs`) that calls the crate's
  decisions: two nodes serving through the finalize, the cleanup racing
  their first acquisitions, up to two restores in place, and an actor's
  delete and recreate. Each of the five mutations is caught.
- **The cleanup's name** is `wasmkeep cluster markers clean`.
- **An old lease's liveness** is judged by the cleanup with a grace of an
  hour, not the nodes' default: the grace the binary that held it ran
  with is not in the bucket, and only a lease past any grace is folded
  and deleted.
- **`genfloor`** is announced only by a node whose version is `0.10` or
  later.
