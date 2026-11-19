# 0028. Actors that end

- Status: accepted (2026-11-14), built in 0.4.2. Sections 1 to 3 (what a deleted actor
  is, the durable delete, the operator delete) are built with this
  record; sections 4 (idle expiry) and 5 (deleting an app) were decided
  here and built in the change after it, with the dated notes at the end
  of each section where the code made a choice the text left open. Revised after an
  adversarial review: a request's outcome written beside it (the key going
  away proves nothing), the fence before a request found carried out is
  reported done, sockets closed only once the delete will run, the marker
  holding the request's id, and a single node's failed durable delete no
  longer reported done on a retry.
- Amended by [ADR 0014](0014-draining-a-node-and-moving-an-actor.md)'s
  amendment of 2026-11-14 (fewer listings at rest): delete requests are
  listed with the other intents when `control/changed` changes, or every
  60 s, not every period. See "Note of 2026-11-14: fewer listings" at the
  end.
- Amended by [ADR 0034](0034-batched-plain-writes.md) (proposed 2026-11-15; if accepted): a reset (every delete) is uploaded only at the acknowledged head with nothing in flight; a batched actor flushes first.
- Date: 2026-11-14
- Release: [0.4.2](../../ROADMAP.md#m4-operating-a-cluster)
- Amends: [ADR 0002](0002-wasmkeep-actor-wit-v0.1.md) (one function added
  to `storage`, `delete-self-durable`, under `wasmkeep:actor@0.2.0`),
  [ADR 0014](0014-draining-a-node-and-moving-an-actor.md) and
  [ADR 0017](0017-moving-one-actor.md) (a second kind of request beside
  moves), [ADR 0022](0022-app-owned-state-and-the-one-time-migration.md)
  (a new family under an app's root, `deletes/`).
- Follows: [ADR 0007](0007-replication-protocol.md),
  [ADR 0012](0012-bucket-garbage-collection.md) (a delete is a reset
  segment, then everything before it is erased),
  [ADR 0020](0020-one-lease-per-node.md) (who may write it).

## Context

Today an actor ends only when its own code calls `storage.delete-self`.
At the end of that event the host:

1. clears the alarm in the node's timer and, under `existing` or
   `internal`, closes the actor's WebSockets with 1000;
2. replicates the event's writes, then removes the local database files;
3. uploads a **reset segment**: one segment holding every page of an
   empty database, built on the current head at the holder's generation,
   so the current chain ends in the empty state (`Replica::ship_reset`);
4. uploads a snapshot of the empty database at that position
   (`SnapshotKind::AfterDelete`) and, once it is acknowledged, collects
   everything before it whatever the retention window (`erase`, ADR 0012);
5. removes the alarm's index entry, only once the reset is in the bucket.

If the files cannot be removed, the empty database is built in scratch
space instead and the actor is given up, so its next event restores from
the bucket, which sets the stale file aside (its sidecar no longer
matches). If the upload in step 3 fails, the actor is given up too: the
delete is lost, and the next event restores the data. `delete-self`
returns nothing and the event's answer has usually gone before step 3:
the caller cannot know.

What is missing (ROADMAP, "actors that end"):

1. an operator cannot delete an actor;
2. no delete is durable: none is answered only once the bucket holds it;
3. an actor nobody uses is kept for good (sessions, carts, rooms);
4. an app cannot be deleted as a whole: `docs/cluster.md` says to stop
   its nodes and remove two prefixes by hand.

Constraints the code imposes:

- The command line holds bucket credentials only (`deploy`, `actor
  move`, `cluster drain`): it never calls a node.
- Only the actor's holder may write its history (ADR 0007, 0020): a
  delete is a write.
- An event's response goes out as soon as the guest sets it, before the
  event ends (`ActorState::request`): a delete recorded at the end of the
  event cannot hold the answer back.
- The last objects of a history may not go: `highest_generation_in_bucket`
  puts an actor's first ownership record above them (ADR 0020, section
  6), and without them a late upload from an earlier holder would become
  the head.

## Decision

### 1. What a deleted actor is

A deleted actor is **a history whose current chain ends in a reset**:
the reset segment and the snapshot of the empty database at its
position. Nothing new is written to mark it: this is what `delete-self`
already leaves, and every rule that protects an acknowledged write
protects it.

- **Who writes it.** Only a holder, through `ensure_owner`: acquire (a
  conditional write of the ownership record at a new generation, ADR
  0020), restore, then the reset on the restored head. Every delete in
  this record, the guest's, the durable one and the operator's, goes
  through the one routine that writes it (`ActorState::delete_now`).
- **What stays.** The reset segment and its snapshot, for good, and the
  ownership record, whose generation never goes down. Erasing (ADR 0012,
  `erase`) removes everything before the snapshot and never these two.
- **What a later event sees.** A new, empty actor at the same address:
  the restore gives the empty database, `on-init` runs again (its marker
  went with the data), no alarm is pending. Under `existing` the address
  still exists in a cluster, as ADR 0010 says ("a delete is not a
  revocation"); a marker the existence check could read is left open
  (below).
- **Why nothing brings the data back.**
  - *Restores* follow the chain from the newest segment of the highest
    generation (ADR 0007). The reset was written by the newest holder,
    so an upload from an earlier holder landing late is on no chain built
    after it. A delete is acknowledged only if its upload completed while
    the hold was valid, the same rule as `exec-durable`: a later holder
    lists after acquiring, so it sees it.
  - *Local files* on another node (an earlier holder's) are used only if
    they are exactly the bucket's head (`Replica::restore`); after a
    reset they are not, and are set aside as `<name>.db.replaced`.
  - *Collection* erases only after the snapshot at the reset is
    acknowledged, with the holder's generation check of ADR 0012, and
    never deletes the reset or that snapshot.
  - *Alarms*: the timer forgets the alarm at once; the index entry goes
    only once the reset is recorded. An entry left behind is delivered to
    an actor that has no alarm, which runs nothing (`er04` in
    `tests/cluster/alarms.rs`).
  - *Moves and takes* restore from the bucket like any acquisition.

### 2. The durable delete: `storage.delete-self-durable`

One function is added to the `storage` interface, after `delete-self`:

```wit
delete-self-durable: func() -> result<_, sql-error>;
```

The package stays `wasmkeep:actor@0.2.0`: an addition keeps the version
([languages](../languages.md#versions)), and a component built before
links as it did. A component that imports it needs a host of this
release.

It deletes the actor **inside the call**, as `exec-durable` writes
inside its call, so that the answer can follow it:

- **In a cluster** it uploads the reset (built from an empty database in
  scratch space) on the replica's head while the hold is valid, and
  returns `ok` only once the bucket acknowledged it. On a single node it
  closes the database, removes its files (the WAL first, then the
  database) and fsyncs the directory. If that fails it answers
  `internal`, storage stays refused (the database is closed), a retry
  removes the files again and answers by its own outcome, and the end of
  the event removes them as `delete-self` does.
- **After `ok`** the actor is deleted. Storage calls later in the same
  event are refused with `not-allowed` ("deleted in this event"),
  `get-alarm` returns none and `set-alarm` and `clear-alarm` do nothing.
  The rest happens when the event ends, as after `delete-self`: the timer
  and sockets as in step 1 of the context, the local files, the snapshot
  at the reset and the erase, the alarm index entry. The actor is then
  given up, so the next event restores the empty database from the bucket.
- **`not-allowed`** inside an open transaction: nothing changes. Commit
  or roll back first.
- **`internal`** (in a cluster): not known to be durable. The upload may still land
  (a request the client gave up on), so the actor may or may not be
  deleted. Replication stops for the actor as after a failed
  `exec-durable`, the event's local writes are not kept, and the next
  event restores whatever the bucket holds. Calling it again is safe.
- `delete-self` itself is unchanged: replicated when the event ends, and
  lost if that upload fails.

SDKs: Rust `storage::delete_self_durable() -> Result<(), SqlError>`, and
TypeScript `storage.deleteSelfDurable()`, which throws the `SqlError`.
The others (Python, Go, C, Zig, D, MoonBit) come later; the API
reference lists them as missing until then.

### 3. The operator delete: `wasmkeep actor delete`

```bash
wasmkeep actor delete --app acme/shop cart/u-1842 --bucket s3://b/p
```

**A request object**, as `actor move` writes (ADR 0017), and not a call
to a node: the command line has bucket credentials only, and a call
would need to find the owner, which a request lets the cluster do.

- **Key**: `state/<tenant>/<app>/deletes/<class>/<name hex>/<id>` under
  the app's root (ADR 0022), empty body. `<id>` is 16 hex digits the
  command draws for each request, so a node that carried a request out
  deletes exactly its key: a second `actor delete` of the same actor
  writes a key of its own, and is never acknowledged by the first one's
  completion (one shared key would be, if it were written again just
  before the node deleted it). The key's last-modified time *T* (the
  bucket's clock) orders requests for the same actor.
- **Listed** by every node of the app on its control pass, once per
  heartbeat period (3 s), beside the moves: one more LIST per node per
  pass. Nodes of other apps never list it.
- **Expiry**: one hour by the bucket's clock, after which the next node
  that lists it deletes it. A request is done long before that unless no
  node of the app serves the class, or none can hold the actor.

**Who acts**, on each pass, at most 16 requests at a time per node, each
in its own task:

1. **The owner**: the node that holds the actor (resident, with a valid
   hold), keeps its record parked (ADR 0024), or has its sockets. It
   queues the delete job in the actor's mailbox, after the events already
   queued.
2. **Nobody**: the record names nobody, or a node that is not live in
   this node's view. Any node serving the class queues the job; its
   acquire is the ordinary conditional one, so one node wins and the
   others find the actor held and leave it to that owner on their next
   pass. A record naming a dead node is free only by ADR 0020, section 2
   (ttl plus grace after that node's last write, or a fence): no fence is
   needed, and `wasmkeep cluster fence` makes it sooner.
3. **Another live node** holds it: nothing here; that node acts.

A draining node acts only on actors it holds; a node that does not serve
the class leaves the request to those that do.

**The delete job**, in the actor's mailbox, runs no guest code (no
instance, no `on-init`):

1. Forget any learned owner. If the actor does not exist (no local
   database and nothing in the bucket), the request is done: "no such
   actor".
2. `ensure_owner`: acquire and restore, or nothing if already held.
   Refused (another node holds it, this node is draining or moving it, the
   bucket did not answer, the secrets cannot be opened): the request
   stays for a later pass.
3. **Replays.** The database's host meta `operator_delete` holds the
   time and id of the last request carried out, `<T> <id>`. If it is
   later than this request's *T*, or this very request (same *T* and id;
   a bucket with second precision gives two requests the same *T*), the
   request was already carried out (its key could not be deleted, or two
   nodes listed it). It is reported done only after the **fence**: an
   upload of this hold's that ends the history (`ActorState::fence`, as
   before removing stale alarm entries). What a restore read is the state
   only once that upload is in: until then a reset that landed late, never
   acknowledged, could be read here while an earlier holder's upload,
   landing later still, becomes the history without it
   (`a_delete_read_back_is_the_state_only_once_the_reader_uploads`).
4. **Sockets**, only now that the delete will run: any still open or
   closing are closed with **1000 "actor deleted"** and the job answers
   `Busy`. Their close events run in the mailbox; the pass waits up to
   5 s for them and queues the job once more. A request already carried
   out never touches the sockets of the actor created since.
5. The delete routine of section 1, with `operator_delete = <T> <id>` in
   the empty database the reset carries. A `delete-self` later carries
   the value over to its own empty database, so a request left behind
   never deletes an actor created after it was carried out. (A node from
   before this record does not carry it: during a rolling upgrade, a
   request whose key could not be deleted can run again on an actor that
   such a node deleted and recreated.)
6. **Done** once the reset is acknowledged: the node writes the
   request's **outcome** beside it, `<id>.done` (`<id>.absent` for no such
   actor), then deletes the request, then hibernates the stub (the hold is
   released). If the outcome cannot be written, the request stays for the
   next pass. The snapshot and the erase follow; a failure there leaves
   old history objects in the bucket, which no restore uses, until the
   actor writes again (`wasmkeep_history_collect_failures_total`).
   Outcomes remain readable and expire after the request TTL (one hour).

**By the actor's state:**

| State | What happens |
| --- | --- |
| Resident here | The job after the queued events; it closes the sockets, their close events run, then it deletes. |
| Hibernated (record released) | Any serving node acquires and restores, then deletes. |
| Parked with sockets (ADR 0024) | The parking node's job wakes it, closes the sockets (their close events run here) and deletes. |
| Resident on another live node | That node acts. |
| Being moved (ADR 0017) | The old owner forwards to the target (`Elsewhere`), so the job is refused there and runs on the target once it holds the actor. |
| Being taken over (owner died) | The record is not free until ADR 0020 says so; then one node acquires and deletes. |
| Restore failing | The job is refused; the request stays until it expires. |

**Sockets: 1000, not 1012.** 1012 tells a client to reconnect, and an
open class would recreate the actor at once from that reconnect. 1000 is
what `delete-self` sends. The operator delete closes every socket of the
actor, open classes included: the operator asked for it to end.

**The command** checks the request before writing it: the actor must
have a history in the bucket (otherwise "no such actor", nothing is
written), and the tenant's layout must be the one of 0.4.0 (as `actor
move`). It then writes the key and waits for its outcome: by default up
to 60 s (`--wait SECS`, at most 3000, below the request's expiry;
`--no-wait` returns at once). `.done` within the wait: it prints
"Deleted" and retains the outcome. `.absent`: "no such actor", non-zero.
`.superseded`: the actor was replaced after submission, non-zero; its data
is left intact. `--resume ID` reads the original operation without filing
a request.
The request gone with no outcome (expired, by a node's clock, or not an
actor address): "dropped", non-zero, nothing known to be deleted. Still
there: non-zero, "requested; the cluster keeps trying until <T + 1 h>".
A request is acknowledged only by its outcome, written after the reset
was recorded, so a "Deleted" is never followed by the data coming back;
the key going away alone acknowledges nothing.

**Mixed versions.** A node from before this record never lists `deletes/`.
During a rolling upgrade the request waits until an upgraded node holds
the actor or finds it free, then runs; the command reports it pending if
that takes longer than its wait.

**Moves.** A delete and a move of the same actor do not conflict: each
runs where the actor is held. A move of a deleted actor moves an empty
actor.

**Metrics.** `wasmkeep_actor_deletes_total{how}` counts deletes recorded,
by `how`: `self` (`delete-self`), `self_durable` (`delete-self-durable`)
and `operator` (`actor delete`). `wasmkeep_actor_delete_failures_total{how}`
counts those that failed to be recorded (the actor was given up, its data
still in the bucket).

### 4. Idle expiry

A class may say how long an actor of it may stay idle before it is
deleted:

```toml
[classes.carts]
expire-after-idle = "7d"
```

and `wasmkeep deploy --expire-after-idle carts=7d` (the same rules as
`--access`: per class, kept by later deploys that do not name it,
`carts=` removes it). It is part of the app version, so a rollback
restores it. The minimum is one hour; seconds, minutes, hours and days.

- **Activity** is any event that runs the actor's code: a request, a
  call, an alarm, a WebSocket open, message or close. An actor with a
  **pending alarm** is never expired (the alarm is activity to come), nor
  one with an **open socket** (its record is held or parked).
- **Finding idle actors cheaply.** An actor writes its ownership record
  when it is acquired and when it hibernates (the release). A record that
  names nobody and was last modified more than `expire-after-idle` ago is
  an actor idle that long, plus the idle timeout. So one LIST of
  `owners/<class>/` gives every candidate with its last-modified time,
  without reading a body, and one LIST of `alarms/` gives the actors
  with an alarm to leave alone.
- **Who collects.** One node per app: the live node with the lowest id
  that serves the class, once every 10 minutes, at most 100 expiries per
  pass and four at a time. Another node takes over when that one stops
  heartbeating.
- **Races with a wake.** The expiry is the operator delete's job with
  one more condition: its acquire must find the record exactly as the
  listing showed it (same last-modified time and holder). A wake since
  then rewrote the record, so the acquire loses or the condition fails,
  and nothing is deleted. Once the expiring node holds the actor, an
  event arriving elsewhere is forwarded to it and queued after the job:
  it finds a new empty actor, as any event after the expiry would.
- **Metrics**: `wasmkeep_actor_deletes_total{how="expired"}`, and the
  candidates found per pass.

*Note, 2026-11-14, as built.*

- **"The record exactly as the listing showed it"** is its version: the
  listing's `ETag` (quotes and a weak marker aside) must equal the one the
  expiry's own read returns, and that read must name nobody; the
  conditional write is `If-Match` on that read
  (`Owners::acquire_unchanged`). One read decides and conditions the
  write, so a wake between a check and the acquire cannot slip through. A
  last-modified time has a precision of a second on S3 and is not
  returned by the read the acquire uses; an `ETag` changes with every
  write. A listing without an `ETag` expires nothing.
- **The job never takes a hold it has.** An actor resident, parked or
  with a socket on the collecting node is not a candidate's victim, and
  the job's acquire never counts a hold already there. After the restore
  the job reads the database's own alarm: the `alarms/` listing is a
  filter, an entry missing from it never deletes an actor with an alarm.
  The operator delete's marker is carried over, as `delete-self` does.
- **The pass's clock is read before the listing**, so a listing slow to
  come back never makes a record look idler than it was.
- **Records found changed or held** are not tried again at that version
  (long-resident actors have old records that the listing cannot tell
  from released ones), so they do not take each pass's budget.
- **The collector** is the lowest id among this node and the live peers
  of the app that serve (not draining, not suspected) and announce the
  heartbeat word `ends`; a node behind on deploys or before its tombstone
  check runs no pass.
- **Format.** A limit is the manifest line `expire <class> <seconds>`,
  in format 4, written only for a version with a limit (a version without
  one keeps its bytes and id). Nodes announce `ends` on heartbeat line 11;
  a deploy writing format 4 is refused while a live node of the app does
  not, as format 3 is for `front`.
- **One node, no cluster**: nothing expires (no ownership records). A
  name whose encoded form was shortened with a hash cannot be told back
  from its record key and is never expired.
- Simulated by `madsim_expiry` (expiry passes racing wakes and alarms, two
  broken variants caught); cluster tests in `tests/cluster/expiry.rs` and
  `websockets.rs`.

### 5. Deleting an app

```bash
wasmkeep app delete --app acme/legacy --bucket s3://b/p
```

1. **The tombstone first**: `apps/<tenant>/<app>/deleted`, created with
   `If-None-Match: *`, holding who and when. From then, `deploy` refuses
   the app, a node refuses to start serving it, and a running node that
   sees it at its next version check (3 s) stops taking events for it,
   closes its sockets with 1001 and stops its alarms. (Amendment of
   2026-11-19: the tombstone is also read every 3 s while a node backs
   off from failed deploy checks, a backoff that reaches 5 minutes.) No node brings
   the app back: actors are never acquired under an app with a tombstone.
2. **Then the data**, once no live node of the app heartbeats (the
   command waits, or refuses; `--force` after `cluster fence`): every key
   under `state/<tenant>/<app>/` and `apps/<tenant>/<app>/` except the
   tombstone, so the history, the ownership records, the alarm index,
   the requests, every deployed version (the deploy history and the
   components) and the sealed **secrets** with them. Keys held outside
   the bucket (the secrets key) are the operator's to destroy.
3. **The tombstone stays.** A node of an older version ignores it, so
   the command refuses while a live node of the app runs a version from
   before this section. Re-using the name is `deploy --app acme/legacy
   --recreate`, which removes the tombstone only once nothing else is
   left under the two prefixes.

*Note, 2026-11-14, as built.*

- **A node stops being live.** A node that sees the tombstone stops as a
  clean stop does, with 1001 instead of 1012 and no hand-over: its actors
  finish their queued events and are released, then its node lease is
  released, so `app delete` sees it gone. It then removes its local files
  of the app (`<data_dir>/state/<t>/<a>/`), and `serve` exits with an
  error. A node refusing to start on a tombstone removes them too.
- **The start race, and lapses.** A node following its app acquires no
  actor until a check for the tombstone, sent while its lease was valid,
  found none: from the moment its cluster is built, and again after
  every lapse of its lease (a node paused or cut off). Either the node's
  lease was written before the command read the live nodes (the command
  waits for it), or its check came after the tombstone (it stops). The
  check after a lapse also compares the `recreated` marker below: a node
  that was away through a delete and a `--recreate` stops.
- **Fencing.** Before it removes anything (and after every round), the
  command fences each node lease of the app still written live, as
  `cluster fence` does: a node past its horizon but not stopped (paused,
  cut off) can then never renew, and never acts again.
- **Late writes.** The command lists and deletes until a listing comes
  back empty at least 40 s (a bucket client's wait for a write, 30 s,
  plus a lease's ttl) after the tombstone and after any node of the app
  last wrote its heartbeat; it also waits for any node that turned live
  meanwhile and refuses one without `ends` that started. `--recreate` is
  refused within that same window. A deploy re-reads the tombstone after
  its switch, and `secrets`, `actor move` and `actor delete` after their
  write (the two requests are taken back): each reports the app deleted
  meanwhile, and what they wrote goes when `app delete` runs again.
- **Local files after `--recreate`.** A node down throughout a delete
  keeps its local databases, and an actor with no history in the bucket
  takes its local file as its first snapshot. `--recreate` leaves
  `apps/<t>/<a>/recreated` (the tombstone's body); a node whose local
  copy of it differs removes its local files of the app before it serves.
- **Refusals.** Besides deploys and node starts, rollbacks, `secrets`
  writes, `actor move` and `actor delete` refuse an app with a tombstone.
  The command refuses the implicit app `default/default`, whose nodes do
  not follow deploys. `--force` removes the data while nodes still look
  live (after `cluster fence`) and beside nodes without `ends`.
- **Waiting.** The command waits up to `--wait SECS` (60) for the live
  nodes, then stops with the tombstone written and nothing removed; run
  again, it resumes (an existing tombstone is not an error).
- Tested end to end in `tests/cluster/app_delete.rs` (nodes stop, sockets
  1001, both prefixes removed, refusals, `--recreate`, a node that kept
  files from before the delete) and by the command line's tests.

## Alternatives

- **An inside call from the command line to the owner.** It would need
  node credentials or a front door for operators, which the CLI does not
  have (ADR 0011 keeps node calls to nodes), and it would have to find
  the owner and handle its death itself. Rejected.
- **A flag on `delete-self` that holds the event's answer until the end
  of the event.** The answer goes out when the guest sets it, and a held
  response would have to buffer a body the guest may still be
  streaming. Rejected for a call that returns once the delete is in the
  bucket, as `exec-durable` does.
- **A delete marker object** read by the existence check and by restores.
  It would make `existing` treat a deleted actor as absent (ADR 0010), but
  it is a second source of truth beside the history and changes
  replication. Left open.
- **Removing every history object** of a deleted actor. A late upload
  from an earlier holder would then be the head, and the first ownership
  record of a new actor there would start from generation 1 again (ADR
  0020, section 6). Rejected.
- **A request that never expires.** A request for a class nobody serves
  would wait forever and fire whenever such a node starts. One hour
  bounds that; an operator who needs longer writes it again.
- **Expiry by the last history write.** An actor that is only read would
  expire while used. The ownership record's release is written for every
  actor that hibernates, so it counts reads too.

## Consequences

- One more LIST per node per heartbeat period (`deletes/` beside
  `moves/`).
- A delete costs an acquire and a restore of the actor if nobody holds
  it: a large actor is downloaded once to be deleted.
- An operator delete or a durable delete that reports done is in the
  bucket; neither can be undone except from a bucket that keeps old
  versions ([backup and restore](../deploy/backup-and-restore.md)).
- `existing` still finds a deleted actor in a cluster (ADR 0010).

## Tests

- **`sim/`**: `madsim_replication` runs owners that delete the actor
  (the reset, the snapshot at it and the erase), by the owner or by a node
  that acquires the actor only to delete it, among takeovers, restores
  and releases, and parked holds woken by their node while another tries
  to acquire for the delete:
  - `deletes_never_bring_data_back` (invariants: every acknowledged write
    is restored unless an acknowledged-or-landed delete after it on the
    chain removed it; nothing a delete removed and that was acknowledged
    before it is restored; the delete itself is kept);
  - `deletes_with_collection_never_bring_data_back` (the same, with
    collection after every snapshot);
  - `a_delete_acked_after_its_hold_ended_can_bring_data_back` and
    `erasing_before_the_delete_is_acked_loses_writes` (unsafe variants
    that must be caught).
  Nodes that restore a delete also acknowledge it as found carried out,
  after the fence. Acknowledging it on the read alone needs an
  interleaving the model does not reach within 20,000 seeds, so the unit
  test above scripts it instead.
- **Cluster** (`tests/cluster/deletes.rs`, plain and TLS): an operator
  delete of a resident actor (done only once recorded, the history erased
  to the reset and its snapshot), of a hibernated one, and of one whose
  owner node lost the bucket for good; an alarm cleared and never fired;
  a request whose key cannot be deleted is not carried out twice (data
  written after it stays, across a restore, and a socket opened to the new
  actor stays open); a mistyped name creates
  nothing; a durable delete answered only once the reset is in the bucket
  (the node cut off right after the answer, another node restores it
  empty), an error with the data kept when the upload fails, and storage
  refused after it in the same event. `tests/cluster/websockets.rs`: a
  parked room's sockets closed with 1000. `tests/alarms.rs`: the durable
  delete on a single node, and its retry after the files could not be
  removed.
- **Unit**: `replication::a_delete_read_back_is_the_state_only_once_the_reader_uploads`
  (why the fence), the marker's comparison (`carried_out`), the request
  and outcome keys.
- **CLI**: parsing, the existence check, the key, the wait and its exit
  codes.

## Plan

| Slice | Size |
| --- | --- |
| This record; the WIT function, host, Rust and TypeScript SDKs | M |
| One delete routine; the durable delete | M |
| `deletes/` keys, the pass and the job; `actor delete` | M |
| Sim cases, cluster and CLI tests, adversarial review | M |
| Idle expiry (section 4) | M |
| `app delete` (section 5) | M |

## Note of 2026-11-14: fewer listings

[ADR 0014](0014-draining-a-node-and-moving-an-actor.md),
[0020](0020-one-lease-per-node.md) and [0009](0009-alarms-in-a-cluster.md)
were amended the same day so that a node at rest lists the bucket about
fourteen times less. What that changes here:

- **Delete requests** are intents: nodes list `deletes/` with the drains
  and moves when `control/changed` changes, or every 60 s.
  `actor delete` writes the marker after its request (after the check
  that takes it back if the app was deleted meanwhile), so a node starts
  on it within a period, as before; a request written by a CLI that does
  not (none exists: `actor delete` is new in this version), or whose
  marker write failed, within 60 s, which can outlast the default
  `--wait` of 60 s (the command then says the request stays, and it is
  still carried out). The node that ends a request (its outcome written, the key
  deleted) writes the marker and keeps the request out of its own copy
  until the request expires, so other nodes stop acting on it within a
  period. A node that still acts on a copy finds the request carried out
  by the marker in the database (section 3), as with a request read
  twice before.
- **`app delete`** reads `nodes/` itself (`all_heartbeats`, a listing),
  as a CLI does, at every step: the nodes' own cadence does not touch it.
  Its 40 s window (`LATE_WRITES`) counts from the app's nodes' last
  heartbeat writes and from the fences it writes itself, both read from
  that listing. Nodes stop on the tombstone they read with each deploy
  check (3 s), not through `nodes/` or `control/`; a stopped node's
  `released` lease is seen by its peers at their next read of the nodes
  they know, within a period, as before. `app delete` therefore writes no
  `control/changed`: the moves and delete requests it removes belong to
  an app whose nodes have stopped, and no other app's node reads them.
- **The collecting node** of idle expiry is the lowest id among the live
  peers this node knows. A node that just started is known to its peers
  within a period through its hello (within 30 s without one, as from a
  node of 0.4.1, which does not collect). Until then two nodes may both
  run a pass, as when views disagree, which the conditional acquire makes
  safe (section 4).

## Stable operator handles

Actor delete prints its request ID before waiting; `--resume ID` never files
another request. The owner checks for a later operator-restore marker, or a
`replaced` delete label whose reset bucket time is at or after the request,
after checking whether that delete was already carried out. Such pending
requests end `superseded`, preserving the replacement actor.

Outcomes remain adjacent to requests without being consumed by readers and
expire after the request TTL. After expiry an ID is unknown and nothing is
filed. App delete resumes match the existing tombstone ID. A 0.5.0 CLI reports
a superseded delete as dropped (fails closed); a 0.5.0 node neither recognises
nor expires `.superseded`. Bindings and durable receipts move to 0.6.0.
See [operation recovery](../operations.md).
