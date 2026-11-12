# 0007. Replication protocol: chained segments and the acknowledgement rule

- Status: accepted (2026-11-02: the madsim replication case passes, and
  removing the acknowledgement rule is caught)
- Amended by [ADR 0020](0020-one-lease-per-node.md) (2026-11-07):
  ownership is a node lease plus an ownership record per actor; where this
  record says lease, read the actor's hold (`LeaseView`: the hold exists and
  the node lease is valid).
- Date: 2026-11-02
- Milestone: [M2](../../ROADMAP.md#m2-cluster-and-durability)

## Context

Leases (`wasmkeep_cluster::lease`) give each actor at most one owner at a time,
under the skew bound. Replication must turn that into the guarantee
WasmKeep promises: **a write acknowledged as durable is never lost, and a
stale owner can never corrupt what the next owner restores.**

Two facts make this harder than "upload files":

1. Ownership is time-based. An upload started while the lease was valid can
   complete after it expired, after another node took over and listed the
   bucket (a slow PUT that still succeeds).
2. A conditional create of a new key succeeds for a stale owner too. Key
   names alone do not fence.

## Decision

### Capture

Actor databases run in WAL mode with automatic checkpoints off. After each
event commits, the replicator reads the WAL frames appended since its last
position, keeps the last version of each page up to the last commit frame,
and encodes one LTX file (ADR 0005) for the next transaction id. Pre- and
post-apply checksums come from per-page checksums kept in memory. Once a
segment is uploaded and the WAL holds only shipped frames, the replicator
checkpoints (`TRUNCATE`).

### Keys: every segment names its parent

```text
actors/<tenant>/<class>/<name>/
  wal/<txid:016x>-<generation:016x>-<parent generation:016x>.ltx
  snapshot/<txid:016x>-<generation:016x>.ltx
```

A segment is transaction `txid` written by lease generation `generation`,
applied on top of the segment at `txid - 1` written by `parent generation`
(0 for transaction 1, on the empty database). The objects therefore form a
tree of histories. A snapshot is the database at one position of that tree.
All writes are conditional creates; fixed-width hex makes listing order
transaction order.

### Acknowledgement rule

A segment counts as **durable** only if its upload completed while this
node's grant was still valid (`grant.is_valid(now)` checked after the PUT
returns). `exec-durable` waits for that. An upload that completes too late is
reported as a failure, and the node stops acting as owner; the write may or
may not survive, which is what an unacknowledged write is allowed to do.

### Restore

1. The head is the newest segment of the **highest generation** in the
   listing.
2. Follow parent links back to the empty database, stopping at the first
   snapshot on the way.
3. Apply that snapshot, then the segments after it. Each segment's
   pre-apply checksum must equal the running database checksum.
4. A missing link or a checksum mismatch stops the restore: the actor is
   marked degraded and refuses events rather than silently rolling back.
5. *(Added 2026-11-07.)* The restore lists the prefix once, after
   acquiring, and fetches the objects that listing names. Every read is
   given up when the bucket stays silent for 5 s (no answer, no next chunk
   of a body, no next page of a listing) and sent again, up to three times;
   then the restore fails and the next event restores from a fresh
   listing. A request the bucket leaves hanging (one held a takeover for
   19.8 s in the real-bucket soak) costs seconds, not the client's 30 s
   per attempt. A read given up is dropped whole before anything of it is
   applied.
6. *(Added 2026-11-08.)* The restore reads up to 8 objects ahead of the
   one it applies, within 32 MiB of them by the listing's sizes (always at
   least one), and a read is also given up, and sent again, once it has
   taken 5 s plus a second per started megabyte of everything read
   alongside it (its own size included) from its request, however it
   keeps moving. An object whose size is not the listing's is refused.
   One node's restores together read at most 32 objects at once, and at
   most 256 MiB of them at once by the listing's sizes, one read taking
   at most 32 MiB of that however large its object (one kill starts many
   restores at once; a larger share would let one read hold every other
   restore behind it). Objects read wait to be applied in each restore's
   own read-ahead (memory stays per restore). A read's time starts once it
   has its turn, and counts every byte the node is reading alongside it,
   asked again while it lasts. Objects
   are still applied one at a time, in the plan's order. In the 12-hour
   soak one restore of 55 objects took 56 s, each read about a second,
   none silent for 5 s.
7. *(Added 2026-11-11.)* A read the bucket refuses with an error a resend
   may get past is sent again after a pause (0.5 s, then 1 s), within the
   same three tries: a GET changes nothing, so a resend is always safe. An
   answer no resend changes (the object is gone, access is denied) still
   fails the restore at once. A restore's reads together end within one
   deadline: 30 s, plus a second per started megabyte of the objects it
   reads (the rate below which one read is given up) and a second per
   eight objects. Its clock runs while the restore waits for the bucket
   to give it the object it applies next, and only then: not during the
   listing (its length is not known before it ends; it keeps the rule of
   step 5), not while an object is applied or the file is put in place
   (local work, never cut), and not while a read waits for the node's
   shared limit on reads before it is asked for (many restores start at
   once after a kill, and a deadline counting their queue would end them
   all half-way). A restore that fails, for any reason, leaves the local
   file and its sidecar untouched (the database is built beside it),
   writes nothing to the actor's history, and gives the lease up as a
   failed restore did before: the next event restores from a fresh
   listing, so it adds no ordering the protocol did not already have.

Why every acknowledged write is on that chain: generation `h` acquires only
after every earlier grant ran out *or was released*, and lists after
acquiring, so it sees every acknowledged upload. An acknowledgement needs the
upload to have completed while the grant was valid; a node releases only
after the actor's state is dropped, so after its last upload completed and
with nothing left to acknowledge. Either way, every acknowledged upload is in
the bucket before `h` can acquire (S3 listings are strongly consistent). The highest generation `m` in its listing restored
a chain containing everything acknowledged before `m` began (by induction),
and only `m` could acknowledge anything after that; generations between `m`
and `h` wrote nothing. So `h` restores, and builds on, a chain containing
every acknowledged write. A stale upload that lands later is on no chain
anyone built on. If nobody wrote after it, it extends the chain it was built
on and counts as a late commit, which is allowed for an unacknowledged write.
Releases make that window shorter: an upload cut short by the event's
wall-clock limit can still land after the next owner listed, if the node
gave the actor up and released right after.

The simulation (`sim/tests/madsim_replication.rs`) checks this with owners
that release (as on hibernation, shutdown and failed uploads), and catches a
node that keeps working and acknowledges after releasing.

### Snapshots and GC

Every 64 segments or 64 MB, the owner uploads a snapshot of its head.
Snapshots off the current chain are never used. GC deletes segments and
snapshots that are not needed to restore the current head, after a retention
window, never on the write path.

## Alternatives

- **Start markers with fencing** (the first version of this ADR): each new
  generation writes `start/<g>` with the txid it restored to, and restore
  drops lower-generation segments above it. **The simulation broke it.** A
  marker can itself land late: generation B's marker upload is slow, B's
  grant runs out and B never runs, generation C restores without seeing it
  and builds on a segment, and then B's marker lands and fences off that
  segment. The result is a hole under C's acknowledged writes (seed 10).
  Chained keys need no markers at all.
- **Highest generation per transaction id, no links**: this cannot tell a
  stale snapshot from a current one at the same transaction.
- **A per-actor head object updated with compare-and-swap after every
  segment**: this fences exactly, but doubles the bucket round trips of every
  durable write.

## Consequences

- `sim/tests/madsim_replication.rs` runs three nodes on the real lease and
  planner code, with crashes, held traffic, skewed clocks and slow requests
  that succeed late. Over 200 seeds of 30 s: about 5,700 acknowledged writes,
  2,300 ownership changes and 97 uploads landing after their grant, with
  every acknowledged write restored. Acknowledging late uploads is caught
  (seed 20; seed 24 since snapshots record their contents and are also
  taken right after a restore, 2026-11-05). The test also fails if late
  uploads stop occurring, so it cannot pass vacuously. Garbage collection
  (ADR 0012) runs in the same simulation. Since 2026-11-07 it runs on node
  leases (ADR 0020, `wasmkeep_sim::node`, whose holds act by the server's own
  rule), with the lease on a task of its own and its writes bounded as the
  server bounds them: over 200 seeds, 7,061 acknowledged writes, 1,073
  generations, 31 uploads landing after their hold ended and 796 fences,
  every write restored (2,000 seeds: 69,952 writes, 7,892 fences);
  acknowledging late uploads is caught at seed 0, after a release at 2,
  after a lapse ended the hold at 0.
- Restore planning is a pure function of key names, in `wasmkeep-cluster`.
- Clock skew beyond the configured bound can break the acknowledgement rule,
  exactly as it can break leases. The bound is a documented operating
  requirement (NTP).
- The simulation found a harness bug on the way: restarted nodes reused reply
  tags, so a dead process's late reply could answer a new request. Tags now
  start at a random offset per client.
