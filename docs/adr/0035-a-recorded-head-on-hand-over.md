# 0035. A recorded head on hand-over

- Status: accepted, as built (2026-11-17), with the deviations in
  "As built" below; proposed 2026-11-15 after two adversarial reviews,
  then one review of the implementation, whose one required fix (a
  snapshot above a listed floor) is applied
- Date: 2026-11-15
- Amends: [ADR 0007](0007-replication-protocol.md),
  [ADR 0020](0020-one-lease-per-node.md) and
  [ADR 0024](0024-socket-home.md)
- Follows: [ADR 0033](0033-data-access-export-and-online-backup.md)
  (restoring past state as a new write) and
  [ADR 0034](0034-batched-plain-writes.md) (acknowledgements and barriers;
  proposed)

## Context

A successful PUT does not mean every S3-compatible bucket immediately
includes its key in LIST. On hand-over, a listing can describe a complete
but old chain. It has no missing link and its checksums pass. An acquirer
can activate from it and write in a newer generation, leaving acknowledged
writes on an abandoned branch.

Today the ownership record carries only a holder and generation.
`Owners::acquire` may replace a record naming this same process, and
`LeaseHold::drop` sends its release from a spawned task. A pending release
or a parked hold can therefore contain information that the bucket does
not yet have. `Owners::resolve` can release a landed acquisition using the
version it reads back. These paths need the head as well as the generation.

`Replica::restore_within` lists once, plans the highest-generation chain,
and can return for an empty plan or a matching local sidecar. The planner
stops at a snapshot; it need not retain the ancestry before it.
`collectable` deliberately removes older history after an acknowledged
snapshot in the current owner's generation, retaining the snapshot and
its segment. Erase does the same after a reset, without a retention wait.
Requiring the original floor object forever would break crash recovery
on a correctly consistent bucket.

There is also an ordering gap: `Replica::acknowledged` checks the lease,
then its caller advances the replica's head. `Pending::end_all` ends holds
synchronously and cannot wait for the asynchronous replica mutex. Merely
asking release to capture the head does not close that gap.

## Decision

A releasing owner records its acknowledged position and checksum in the
conditional write that frees the actor. An acquirer must prove that its
restored state covers that position before activation. Record the ownership
generation in which that requirement was established too: a verified
later generation can prove descent even after collection removes the
original objects. Preserve the requirement across failed acquisitions,
pending releases and same-process wakes.

This is a minimum required position. Later unacknowledged uploads may be
retained when they extend the verified chain, as ADR 0007 permits. Neither
a larger txid nor a generation larger than the *head's* generation alone
is proof. Section 3 defines the narrower generation rule.

Strongly consistent GET and conditional writes remain requirements for
ownership and node leases. Strongly consistent LIST remains the supported
bucket contract, especially for crash recovery, migration, collection and
offline reads. The new check protects complete hand-overs from some stale
listings; it does not introduce an eventual-consistency mode. A diagnostic
pass samples that contract and cannot prove it for all future operations.

### 1. The recorded requirement

Use `wasmkeep-owner 2` at the existing ownership key. For example, a
released hold of generation 42 that restored head 41:187 without advancing:

```text
wasmkeep-owner 2
- -
42
head 41 187 09abcdef01234567
set-at 42
complete
idle-since -
```

The holder is `node instance`, or `- -` when released. The third line is
the ownership generation. `head <generation> <txid> <checksum>` identifies
an acknowledged database position; the checksum is the LTX database
checksum, exactly 16 lowercase hexadecimal digits. `empty` denotes the
explicit pre-history position, never an empty listing. `unknown` means
there is no recorded position. `set-at <generation>` is when this floor
was established, not the generation of its history object. Acquisition
copies this stamp unchanged; it must not move the stamp forward merely
because ownership changed. `unknown` uses `set-at 0`.

The completeness line has three forms:

- `complete`: the releasing owner has stopped acknowledging; the head
  covers all its acknowledgements and the predecessor requirement it
  verified. It is valid only on a released record.
- `complete-inherited`: a held record preserves a complete predecessor
  requirement. This hold has not been allowed to upload new history.
  A crash or cancelled restore in this phase does not lose completeness.
- `lower-bound`: the holder may have acknowledged beyond this head.
  `unknown` is always a lower bound.

The final `idle-since` line preserves idle-expiry eligibility as section 3
specifies; its integer form is a nonnegative, nonoverflowing Unix timestamp
in milliseconds. Reject missing or duplicate lines.

For a known head, require `0 < head generation <= set-at <= ownership generation` and a nonzero txid. For `empty`, require a positive stamp no
higher than the ownership generation. Reject malformed or extra fields,
overflow, unknown versions and invalid combinations. Recognize migration
tombstones separately, as section 5 requires. Version 1 and absence carry
no head evidence; existing generation seeding and migration guards still
apply. Neither proves that the actor has never existed.

A successful acquisition writes the inherited requirement into its new
record. A complete predecessor becomes `complete-inherited`; a lower
bound stays a lower bound. Before the first new history upload, after
restore verification, conditionally change `complete-inherited` to
`lower-bound`, keeping its head and stamp. Update the hold's version only
on that successful transition. If the outcome is unknown, do not upload:
read back the exact holder, generation and payload, or leave an unresolved
transition to the resolver. Failure or conflict cannot authorize a write.
No event that may write, first snapshot, reset or generation fence may
precede this transition. Periodic snapshots are also deferred until demotion
is confirmed, even at the unchanged restored head. Restore currently seeds
`since_snapshot` from the plan's segment count, so a due snapshot must pass
this gate too; being due is not permission to upload.

This adds at most one ownership PUT per acquired hold that begins writing,
not one per segment. It is necessary to preserve completeness during an
acquirer's retry window without claiming completeness after a crash that
hid new acknowledgements. Keeping the flag until an acknowledgement only
in memory would not be safe. A read-only hold can remain inherited.

On release, an unactivated hold preserves the requirement and stamp; an
inherited complete requirement becomes `complete` again. A hold that has
verified and frozen its acknowledged head may publish it as complete,
with `set-at` equal to its own generation. The head may still be in an
older generation. Merely releasing an unverified lower bound cannot make
it complete. Unknown stays unknown until a supported restore verifies the
state and the hold is ended with its acknowledged position known.

### 2. Local ordering, release and wake

Make the hold cell the authoritative home of the acknowledged head,
checksum, inherited requirement, verification state and ended state.
Use its short synchronous mutex for both acknowledgement and ending a
hold. After an upload succeeds, the acknowledgement operation locks this
cell, checks that the hold exists and its lease epoch and deadline are
valid, and advances the head and checksum before unlocking. No await or
local-file write belongs in that critical section. The replica may keep
a copy for replay and sidecars; release never reads that copy as authority.
After successful restore, initialize the cell's verified head and checksum
under this lock, rechecking validity before allowing activation. Until
then it retains the inherited requirement, never an older candidate.
The first snapshot and reset use this same operation. Separate advancing
an acknowledged database position from checking an upload's validity:
`put_once` currently calls `acknowledged()` for periodic snapshots too.
That path checks the cell and lease but never advances the head, checksum
or stamp. An after-delete snapshot likewise confirms the existing reset
position rather than acknowledging a new position.

`end_all`, drop, park and explicit release take the same cell lock to end
acknowledgement and freeze the payload. If acknowledgement wins, its head
is included even if the caller has not yet received its reply. If ending
wins, completion is `Late` and cannot produce a successful durable reply.
A lease invalidation after the validity check may end the hold, but its
capture waits on the same cell and includes that advance; an invalidation
before the check refuses it. Epoch changes never revive an ended hold.
This closes the gap without making the lease task await a replica mutex.
Use a fixed lock order for the hold registry and cells; acknowledgement
must not acquire the registry while holding a cell.

For clean hand-over, flush before ending. With ADR 0034, keep `Flushed`
and `GaveUp` barriers. Captured or in-flight work, and a plain write's
successful reply, are not a durability acknowledgement. A batch advances
the head once. A failed flush or lapse can still publish a complete head
if the inherited requirement was verified and this critical section
froze all acknowledgements. If metadata is unavailable, retain a lower
bound instead. ADR 0020's no-release rule on loss of the node lease stays.

Ending must register the frozen head by actor key *before* removing its
live cell or spawning its release task. Keep it registered while the
release is queued, in flight or pending; a count of releases in flight is
not enough. Parking transfers the same metadata without a gap. There is
no extra ownership PUT just for parking.

Under the existing per-actor acquisition/resolution lock, take the maximum
of the record's requirement and **every local ended, pending, in-flight
or parked head for that key**, before sending an acquisition. Here maximum
means the furthest proven position on one acknowledged chain, not a blind
numeric maximum across branches. Local heads retain their stamp and the
proof of the inherited requirement they covered. Equal identities must
have equal checksums; incomparable requirements refuse acquisition rather
than discard either. Unknown supplies no position but cannot erase a
known one. The combined requirement is complete only if a frozen local
head covers the outgoing hold's entire acknowledged state and every
inherited requirement, or the complete bucket requirement already covers
all local heads. Otherwise preserve lower-bound status. Retain the stamp
of the requirement that supplies the chosen head; for equal heads retain
the later stamp, never weaken it just to admit an older witness.

Use a synchronous per-key metadata lock for end/park registration and the
acquisition's local snapshot, within the asynchronous actor lock on the
acquiring side. Before taking that snapshot, reject acquisition if a live
acknowledging cell remains. Ending transfers its head into the registry
under the metadata lock before clearing that cell. Thus an acquirer sees
either a still-live hold and waits, or its frozen head; no outgoing head
can appear only after the acquisition has been sent. Keep network I/O
outside this synchronous lock and the frozen entry registered throughout.

This requires changing the present ordering, not just adding fields.
`Pending::end_all` currently drains the live registry before taking cell
locks; it must instead freeze and register each cell under the per-key
metadata lock before removing it. A global drain may mark entries ending,
but those entries must remain visible and block acquisition until frozen.
`LeaseHold::take` must also take metadata before the cell, register the
frozen payload, then empty and unregister the cell. Use that same order
for park, lapse and drop; acknowledgement takes only the cell lock.

Add a preparation hook inside both `Owners::acquire` and
`acquire_unchanged`, after the authoritative GET and eligibility checks
but before constructing and sending the conditional PUT. Under the per-key
metadata lock it checks live/ending entries, applies the retirement rule
below, combines requirements and reserves an acquisition entry containing
the exact attempted bytes and epoch. `after_wake`, which runs after the
write today, is too late. Keep that reservation visible until registration
or resolution; recheck epoch and cancellation before sending. A lapse after
preparation ends the reservation as well: a late successful PUT goes to
resolution, never into an active cell.

Carry that combined requirement in the attempted acquisition bytes and
in its unresolved entry. Only then may a successful wake discard its
parked entry. If the wake lands but its reply is lost, both read-back and
`resolve` retain the combined head, including the parked head, rather than
reconstructing a release from the older bucket floor. An old release that
ends as `Superseded` may drop its metadata only after a successor has
inherited it, the bucket is proven to cover it, or the remote-supersession
rule below applies. Process-local evidence
is lost on a process crash; section 4 states that boundary.

Retire stale local evidence before comparing chains. Each entry retains
its originating holder instance and **ownership generation**, separately
from its head generation and stamp. A fresh, decoded ownership GET showing
another holder at a strictly higher ownership generation supersedes that
entry. A released record at a strictly higher generation also supersedes it:
the successor has ended. For a successor acquired by this process, the
local transfer described above is mandatory before its acquisition PUT;
its release cannot be used to excuse a missing transfer. For a remote
successor, retirement does not require an ancestry comparison. An attempted
local acquisition that lost its conditional-write race is not evidence
that the resulting generation belongs to this process. Apply the
rule to ended, parked, in-flight and unresolved entries under the actor
lock; cancel their release retries and remove their head requirements.
A response from an already in-flight release cannot reinsert them.

This retirement accepts the supported crash-takeover decision; it does not
claim that the remote successor's chain proves a local head under arbitrary
LIST lag. After a lapse and remote takeover, reacquisition follows the
remote record, including its completeness, rather than comparing it with
a stale local branch forever. Same/lower generations, absence, unreadable
records and failed GETs prove no supersession. A `Superseded` enum alone
is insufficient: today's conflict reader can return it for an unreadable
record. Never retire evidence on that result without the fresh proof.

Ordinary release retries use identical bytes and the version frozen when
the hold ended. They never adopt a newer owner's version. The resolver is
a distinct exception: under the actor lock, with no live hold at that
acquisition, it may read the landed acquisition's version and create its
release. It must match this process, exact generation and expected phase
and payload, and combine retained local requirements before releasing.
For inherited-to-lower-bound transitions, use a separate resolver entry,
registered under the metadata and cell locks **before** sending demotion.
It carries the key, holder instance, generation, pre-demotion version,
exact before/after payloads, epoch and eventual frozen release payload.
Ending by lapse, park or drop transfers this entry with the frozen head;
it must not reduce it to an ordinary release on the old version. No upload
or execution is permitted while its outcome is unknown.

Under the actor lock, the transition resolver reads the record. If it is
exactly the before state at the saved version, an ended hold may release
conditionally on that version. If it is exactly the after state for this
holder and generation, it may construct a release on the version just
read. Both release the same frozen requirement, with completeness derived
from verification and ending, not guessed from the demoted flag. Each
constructed release has fixed bytes and condition. If demotion lands
before the old-version release, that release conflicts: return to the
transition resolver, recognize the after state and construct the second
release. If the release lands first, the delayed demotion conflicts and
cannot restore a holder. A matching released payload ends the entry.
A live hold may adopt a confirmed demotion only after a cell/epoch check;
a parked or ended hold is released, never revived by read-back.

Failed reads remain pending under the existing bounded resolver retry
schedule. A changed holder or generation is never overwritten; apply the
retirement/transfer rules before discarding local evidence. Unexpected
same-generation bytes fail closed as a protocol error. Merely matching
holder and generation, as `still_this_hold` does today, must not send an
old-version release into endless retries. The transition entry remains
until release, proven supersession or fencing under the node-lease rules;
loss of the lease still prohibits new resolver writes.

### 3. Proving the floor and bounding discovery

Acquisition grants exclusivity, not permission to run. Obtain the record
and combined local requirement first. Then select the usual highest-
generation restore plan, and prove it before the empty-plan or local-file
return. There are three ways to prove a known floor:

1. The selected chain contains its exact generation and txid. Restore and
   compare the checksum there, then verify the subsequent chain. A
   snapshot exactly at the floor is a valid base after checksum validation.
   When needed, planning must expose ancestry beyond its usual snapshot
   stop. An unrelated larger txid is not proof.
2. A validated segment or snapshot **on the selected chain** has generation
   strictly greater than the floor's `set-at` stamp. Every such generation
   could upload only after verifying its predecessor requirement. By
   induction it covers this floor, even if its original object is gone.
   A key name alone is not validation: validate the object and replay
   checksums and links from the selected base through the head. The
   witness must predate this acquisition; the acquirer cannot manufacture
   a fence or snapshot to prove its own restore. No arbitrary object on
   another listed branch qualifies.
3. A local database has a sidecar at the exact recorded identity, no
   pending WAL, and a freshly computed database checksum equal to both
   the sidecar and the recorded checksum. This proves the floor without
   LIST containing its key. For a complete requirement it is a usable
   base even if LIST is empty or behind. Replay any selected descendants
   only after proving their connection; a conflicting branch is an error.
   A local file below the floor, a different checksum or an unvalidated
   sidecar proves nothing. For a lower bound it proves only the floor;
   the strong-LIST recovery rule still selects the current state.

The generation rule is deliberately relative to `set-at`, not to the
head generation. Suppose A releases H with stamp g. B acquires g+1,
verifies H, writes 64 segments, snapshots at P and collects below P, then
crashes. C can restore P in generation g+1 without finding H. In contrast,
an object in generation g uploaded before A froze H does not prove H.
The rule depends on capability gating: no older writer that skipped this
verification may produce a generation after a v2 floor was established.

An explicit empty requirement has no nonempty floor. Unknown follows
section 4. Neither permits bypassing generation seeding or app replacement
guards. A complete inherited head proves acknowledged state, not the
absence of later unacknowledged extensions.

For missing visibility, use this schedule:

- At most **10 attempts**, the first immediately. The nine intervening
  delays are **100, 200, 400, 800, 1600, 1600, 1600, 1600, 1600 ms**.
  Add independent uniform jitter from zero to 20 percent of each delay.
  Delays total at most 11.4 s; requests consume the remaining time.
- One **15 s monotonic discovery deadline**, starting before the first
  listing, covers all listing pages, planning, floor-proof reads, their
  permit waits and backoffs. An attempt counts when started, even if a
  page or proof read never completes. Stop on the first exhausted bound.
- Apply an outer deadline to the entire listing stream, not merely each
  page. Today's `list_sized` (`list_listed` since 0.9.0) has a five-second idle rule and continues
  indefinitely while pages arrive; retain its idle/error retries only
  inside the remaining outer time. Cancel the stream at the deadline
  even if it is still making progress. Sleeps and proof-read requests
  are capped by that same remaining time. Release permits before sleeping.
- Retry a behind listing, missing link or named object's not-found with
  a fresh whole listing. Do not union attempts or choose a lower competing
  branch to pass the check. Malformed LTX, checksum mismatch and proven
  conflicting ancestry fail immediately as corruption. Missing evidence
  alone is not evidence of corruption.

Discovery finds the candidate identities and structural proof. Full LTX
validation and database checksum comparison happen during replay, before
the gate can pass; discovery does not download a large snapshot twice or
require its entire replay to fit in 15 s. Any extra ancestry metadata reads
needed to select that candidate remain discovery work. Planning must check
cancellation between bounded pieces of work, as listing does between pages.

A structurally suitable plan is replayed into a temporary database with
the existing read limits and one replay allowance: `RESTORE_BUDGET`
(30 s by default), plus one second per started MiB and one second per
started group of eight objects.
As today, shared replay-permit waits and local file work are excluded from
that read allowance. This is not a 30-second wall-clock restore bound.
Discovery's 15 s is additional; proof reads are not given another replay
budget. Local checksum work, like replay, must finish before acceptance, but is
not cut short by the discovery deadline once a candidate has been selected. Do not interrupt
local installation midway through filesystem changes merely to meet a
network deadline.

If replay finds a visibility miss, discard its temporary result. It may
return to discovery only within the original 15 s deadline and ten
attempts. Retain the replay time already consumed; retries do not reset
its allowance. For a changed plan, recompute the size allowance for that
plan and subtract previously consumed replay time. A held read permit is
released on cancellation. Before installation and activation, recheck the
hold and the proof. No event, alarm, reset, first snapshot, generation
fence or collection may run while verification is pending.

Caller limits remain separate and are not extended:

| Existing limit | Interaction with this gate |
| --- | --- |
| `FORWARD_TIMEOUT`, 30 s | Bounds a forwarded request, including the target's acquisition and restore; 15 s discovery plus replay can exceed it. Propagate the remaining deadline to the target and cancel abandoned work. |
| `HAND_OVER_WAIT`, 30 s | Bounds the queued drain/hand-over job; its timeout does not create a fresh acquisition budget. |
| `EXPIRE_WAIT`, 60 s | Bounds the queued idle-expiry job, including its conditional acquisition and restore. |
| `limits.wall`, configured | Bounds guest execution and the existing wait for an old database connection to close; it is not currently a whole-restore deadline. Carry any caller's remaining deadline through both waits and execution. |
| `MOVE_WAIT`, 30 s | The source's wait for the queued move job; not a fresh restore allowance at the target. |
| `MOVE_DEADLINE`, 30 s | The source's temporary routing hint after move-out; not an end-to-end bound on a move intent or a large restore. |
| `DELETE_WAIT`, 60 s | The wait for each submitted delete job, including acquisition and restore in that job. The existing busy/socket-close retry may submit another job. |
| Request cancellation or hold lapse | Ends waiting; no retry renews that hold or grants permission to activate. |

If an operation has a remaining caller deadline, use the earlier of that
and the applicable discovery/read deadline. A timed-out queued job must
check cancellation before any later activation or mutation; a detached
waiter is not permission for an abandoned job to write. Size-dependent
replay may legitimately exceed a move's routing hint, so callers must not
report a move complete merely because the source released its record.

Introduce a distinct retryable visibility-refusal error. Map it, discovery
exhaustion, lost holds and unknown-head consistency refusal through
`state.rs` to `OwnershipError::Unavailable` (HTTP 503 with the existing
retry guidance), not the current catch-all `Broken`. Keep malformed
records, invalid LTX, checksum conflict and local-file faults as their
existing non-visibility failures. Give up the hold with its inherited
requirement intact; a later request may retry but cannot lower that floor.

A refusal is not free: a fresh cycle can acquire, bump the generation,
perform ten whole listings, then release. Keep a per-actor refusal entry
outside the resident actor's lifetime, shared by requests, alarms, move,
delete and expiry on that node. Coalesce concurrent work. After successive
visibility refusals, wait at least 1, 2, 4, 8, 16, then 30 seconds between
cycles, with zero-to-20-percent positive jitter. Requests during cooldown
return unavailable with the remaining retry delay, without acquiring or
listing. Reset only after verified activation or app replacement, not on
release, eviction or a new request. Keep entries until reset or shutdown;
apply admission backpressure rather than evicting them to evade cooldown.
Existing pending-release retries remain separately bounded and never start
a new acquisition. Per running node and actor, each admitted cycle permits
at most one generation increment and ten listings; a cluster of N nodes
can still incur N times that cost, and restart loses this local throttle.
This is a load bound, not a consistency or cluster-wide rate guarantee.

Refused acquisition/release must not restart ADR 0028's idle clock merely
by rewriting the owner key. Add a required v2 line after completeness:
`idle-since -` or `idle-since <unix-ms>`. The dash uses that released
object's bucket last-modified time; an integer preserves an earlier idle
origin. On acquiring a released record, capture its effective idle origin
from its field or GET metadata and carry it through demotion, unresolved
writes and a refusal release. Until verified activation, preserve it even
across crashes. A verified activation clears it to the dash, so a subsequent
ordinary release starts a new idle period. V1 conversion captures the
predecessor's bucket timestamp; never invent one from the local clock.
Extend the ownership read result to carry last-modified metadata from the
same GET as its bytes and version; today's `(body, version)` result alone
cannot supply it. A missing timestamp refuses acquisition when needed.
For an absent record or a held predecessor with no carried idle origin,
use the dash; a held predecessor with an integer keeps it until activation.
Clearing the field at activation is local until the next required ownership
write; it adds no PUT, and a crash may conservatively retain the old age.
Expiry still checks the verified database's alarms and sockets.
Expiry uses the effective idle origin for eligibility, then retains its
existing exact-version check. LIST supplies candidates, but v2 readers
must GET their idle field before excluding them as too recent. Refusal
keeps an actor eligible; it never authorizes deletion without verification.

For `actor move`, the target's take is refused until restore passes; it
must not answer `Held`, arm an alarm or run the actor on a failed proof.
The source clears its temporary routing hint on a failed take and retains
the move intent for a later pass, as for other failed takes today. Another
node may acquire only through the same gate. The CLI must distinguish a
submitted intent from a completed move, show refusal/timeout as such, and
never treat it as success or automatically force a takeover. The intent's
own expiry still applies.

Log the actor, acquired generation, required head and stamp, last observed
head, attempts, elapsed time and refusal reason. Use bounded reason labels
in counters, not actor identities or checksums. Count retries and visibility
refusals separately from corruption.

### 4. Crashes and incomplete evidence

A crash after the inherited phase was durably changed to `lower-bound`
can hide later acknowledgements. Fencing, expiry margins and fresh GET
prove that the predecessor cannot acknowledge more; they cannot prove
that LIST exposes everything it already acknowledged. Repeated equal
listings and waiting 15 s prove no such thing under unbounded lag.

For supported strongly consistent storage, keep ADR 0020's crash takeover
and ADR 0007's post-acquisition listing argument, additionally checking
the recorded lower bound with section 3's generation rule. Version 1,
unknown and absent records use this same storage assumption and must not
be described as complete hand-overs. A known consistency failure refuses
this recovery path as unavailable. A passing diagnostic is not independent
proof of consistency.

A crash in the acquisition/restore window preserves `complete-inherited`
in the bucket, so another acquirer retains the complete head and can wait
for it. A crash after the phase change but before the first acknowledgement
conservatively leaves a lower bound. Parking records no new head in the
bucket: a parked-process crash normally has only that lower bound, although
a hold that never left the inherited phase still preserves completeness.
Same-process wakes use the stronger local evidence from section 2.

Undetected LIST lag after a writing owner's crash remains outside the
guarantee. Covering it needs a separately reviewed protocol that publishes
a head before every durable reply, or a provider-guaranteed visibility
bound after fencing. This proposal does neither.

### 5. Compatibility, enablement and rollback

Version 1's decoder ignores trailing lines but rejects a different header.
Appending the head to v1 would silently lose it. V2 therefore changes the
header; old server paths treat it as corrupt, including ownership routing
and idle-expiry reads, not just acquisition. This is a rollout constraint,
not a compatibility strategy by itself.

Introduce the node capability word **`recorded-head`**, advertised from
the binary's ability to read, preserve and enforce this protocol. New
readers accept v1 and v2 throughout ownership, routing, expiry, release,
resolution and operator tooling. Before enablement, new servers continue
to write v1. Merely seeing one capable peer never enables v2 writes.

Store the durable one-way setting at
`formats/<tenant>/<app>/recorded-head` under the configured bucket prefix,
with the exact body `wasmkeep-recorded-head 1\n`. Create it conditionally;
absence means disabled, the exact body means enabled, and unreadable or
unknown content refuses service. This is outside both `state/` and `apps/`:
state replacement, app deletion, `--recreate`, deploy restoration and
component rollback must neither delete nor overwrite it. It is not part
of a deploy version or a backup's replaceable payload. Check it on startup
and before acquisition, including after a lease lapse. Backup may report
it, but restore must preserve an enabled destination regardless of what
the backup says; importing v2 ownership metadata requires enablement first.
A capable but not enabled node that encounters v2 fails closed and reports
a setting mismatch; it never writes v1 over v2, including during release
or resolution. Seeing v2 does not silently enable the app.

Enable per app using the
capability-check pattern of ADR 0034 and the stop/start discipline of
ADR 0020 section 6. Upgrade every serving node and the CLI first. Stop and
drain the app, verify no live leases remain, and wait the existing late-
write safety window. Check the app's registered nodes for `recorded-head`;
retire or upgrade older registrations and account explicitly for any
unregistered server. Persist enablement before restarting capable nodes.
Require that setting before writing or converting any v2 record. Conversion
uses the exact read version; a v1 predecessor starts as unknown, never as
an invented complete head from a listing. New records use v2 thereafter.

Do not infer enablement from a transient membership snapshot or switch
back to v1 when a peer disappears. An enabled app rejects service by new
nodes that detect an incompatible peer and reports the operator error.
Old binaries do not understand the setting: deployment/start controls must
exclude them. If one is accidentally restarted, v2 records remain
unreadable to it, but it could still reach an unconverted or absent record.
The rollout must not claim that the new header alone prevents that case.
Running such a node after enablement is unsupported and requires stopping
it, not bypassing the floor or silently disabling the feature.

Before enablement, rollback to v1 binaries is ordinary rollback after
stopping new nodes: no v2 bytes were written. After enablement, rollback
means a binary that still reads and enforces v2. There is no automatic
v2-to-v1 downgrade. Going further back requires a separately designed,
stopped conversion that preserves generations, fences late writers and
explicitly gives up this guarantee. Never delete records, strip fields or
lower generations to make an old binary start. Component rollback does
not roll back the app's ownership format setting.

The `wasmkeep-owner moved` marker (`legacy_layout.rs` since 0.9.0) is **not** an unknown
ownership version. Dispatch that header to the migration-tombstone reader
before v1/v2 decoding, retaining the target and optional `unowned` form.
It stays non-acquirable at the old location; conversion must not turn it
into a free record, seed history there or follow it as authority to write.
Unknown numeric versions still fail closed. Migration preserves v2 payloads
at the destination and leaves the existing tombstone at the source.

Update CLI readers and status/inspection output to name both formats and
report the head, stamp and completeness when available. Use the matching
CLI for migration, expiry inspection and any command that handles records.
Old readers can reject or omit v2 records; their output is not evidence
that an actor is absent. `cluster diagnose` currently probes storage
semantics, not every actor's recorded-head safety. Keep that distinction:
its success, from an old or new CLI, neither enables v2 nor certifies that
old servers enforce it. Add an explicit format/capability report to the
new tooling; document the minimum CLI/server version at implementation.

### 6. Restore, erase and replacement

ADR 0033's single-actor restore first acquires and verifies the live head.
It writes the chosen past content as a new replacement at current txid + 1,
with the live head as parent. The reset/replacement uses the acknowledgement
critical section. Release records its new identity and checksum, never the
historical source position. `--if-head` compares the verified live head.
ADR 0034's flush and reset guards still apply.

Operator delete and idle expiry use this same gate through
`ensure_owner_with`, including the `acquire_unchanged` path used by expiry.
Keep its requirement that the record is released and matches the listed
version, then combine local evidence and verify restore before reading
delete markers, checking alarms or writing the reset. Refusal releases the
inherited requirement, preserves idle eligibility and enters the shared
cooldown. It must not report deletion/expiry success, prune history or
bypass the floor because the requested end state is empty. Existing socket,
alarm and delete-intent guards still apply after verification.

Erase likewise verifies before resetting. After acknowledgement it keeps
the reset segment and snapshot while pruning older data, as ADR 0012
requires. Do not retain erased data merely to prove an old floor. A crash
before release leaves the inherited stamp; the reset in a later ownership
generation proves descent even when all pre-reset history has gone. A clean
release instead records the reset itself with the current generation stamp.

Whole-app restore is an explicit stopped replacement, not an ordinary
hand-over. Keep ADR 0033 section 5's start barrier, local-file invalidation,
late-write wait and generation markers. It writes **no ownership records**.
Do not copy floors from the old app or synthesize complete v2 records from
the backup. The first acquisition seeds above the restored keys and marker
and starts with unknown evidence, using the supported strong-LIST contract.
This is an intentional limit of the hand-over protection. Actors absent
from an in-place backup still receive the prescribed generation markers
and start empty; old sidecars cannot reintroduce their state. Preserve an
enabled app's format setting across replacement, and require compatible
nodes when opening it again. A later verified release establishes its first
complete v2 floor. In-place `--deploy backup` restores code and secrets,
not `formats/<tenant>/<app>/recorded-head`; check compatible serving
binaries before removing the restoring barrier. `app delete --recreate`
also retains that key even when both app and state prefixes are empty.
The recreated app's absent owner records must therefore start in v2.
Amend ADRs 0028 and 0033 to state these rules explicitly.

Offline history, export and backup gain no current-head guarantee here:
they do not acquire ownership and retain their LIST assumptions.

## As built (0.5.0)

The decision above is built, off by default and enabled per app with
`wasmkeep app format --enable`. Where the build differs, the review of
the implementation judged each difference acceptable:

- **(a) No `complete-inherited` phase is written.** A held record stores
  `lower-bound`; only a verified hold's release writes `complete`.
  Nothing reads completeness: every requirement is a minimum floor the
  gate must prove, so the pre-upload demotion write, simulation case 15
  and the demotion half of case 6 are not needed. The cost is liveness
  only: the "complete requirement is a usable base" sidecar shortcut of
  proof 3 is not built either, and a future one must trust only released
  `complete` records.
- **(b) The setting and the peers' capability are read once per lease
  epoch,** on the first acquisition (and again after a format mismatch),
  not at startup and not on a timer.
- **(c) No outer deadline on one listing stream.** Attempts are bounded
  (ten in 15 s), a single listing keeps its idle rule, and caller
  deadlines are not propagated.
- **(d) A local file at the selected chain's head is used** when the
  listing alone proves the floor by a later generation, before the replay
  validates that witness.
- **Proof 1 beyond the snapshot stop** is structural: when the plan's
  base is a snapshot above an exact floor, the floor is proven if it is
  listed and the base links back to it through listed segments (each
  naming its parent generation). No checksum is compared at the floor on
  that route; nothing below the base is replayed.
- **Local evidence** is dropped when the ownership record is absent
  (only a delete and recreate or a whole-app restore removes one, and
  either removes the history it names).
- **Known limit:** a clean stop gives each actor 3 s for its release,
  then writes the node lease `released`; actors whose release has not
  landed take the crash path, with the floor last recorded.

Applied amendments: the headers of ADRs 0007, 0020 and 0024 point here.
The rules this record states for ADRs 0012, 0028, 0033 and 0034 are
stated here and hold as built.

## Verification

Built and checked by: codec fixtures (both versions, phases, holder
tokens, tombstones, the frozen old decoder); hold-cell, local-heads,
`prove`, `combine` and `linked_back` unit tests; cluster tests of the
real path (a lagging listing refused with 503 then served on two real
nodes, a collected floor proven by a later generation, a replacement
recorded, refusal beside an incapable peer, the format setting across
delete and recreate); and simulations for cases 2, 3, 4 (in part), 7,
8 (with the snapshot-above-floor interleaving), 12, 16 and 17. Cases 1,
5, 11, 13, 18 and the crash half of 6 are covered by real-code tests
only; 9, 10, 14, 15 (moot), 19 and the full broken-variant list are left
to the testing pass, as is a real-bucket hand-over check.

The original plan follows. No simulation or implementation result was
claimed by this record before it was built. Extend
`sim/tests/madsim_replication.rs` and the shared ownership model, using
ADR 0034's split upload/acknowledgement model where needed. Independently
delay LIST while GET, PUT and conditional writes complete. Model snapshots,
collection, erase and a logical state digest; real LTX checksum coverage
belongs in the real-code tests.

### Simulation cases

1. Hide released WAL and snapshots for five seconds across move, hibernate,
   drain and stop, including an apparently empty listing. Wait and recover
   all acknowledged writes; repeat with permanent lag and bounded refusal.
2. Re-acquire in the same process while release H is queued, in flight and
   pending and the bucket still has F < H. The new record must contain H;
   the old release becoming `Superseded` cannot lose it.
3. Park at H, then wake with an acquisition that lands but loses its reply.
   Run `after_wake`, resolution and the old parked release in both orders.
   Every successor or resolved release retains H, including cancellation.
4. Race `end_all` with PUT completion at every point around validity check,
   head advance and reply. The frozen release includes any successful
   acknowledgement; completion after ending is late. Also race lease epoch
   change, failed flush, batched completion, drop and clean-stop timeout.
5. A busy owner acquires over H, writes 64 segments, snapshots at P,
   collects H and crashes on a strong store. The next owner **must activate**
   from P. Repeat after erase and with several generations and failed
   acquisitions. Test same-generation objects before H cannot be witnesses.
6. Crash in the acquirer's listing/backoff window, after acquisition lands
   without its reply, and after a failed restore. Preserve complete-inherited
   through each takeover. Separately crash before/after the durable phase
   change and after its lost reply; no upload precedes confirmed demotion.
7. Release reply lost, release delayed past two acquisitions, and resolver
   with a fresh read version: preserve payload, never overwrite a newer
   hold. Include local heads missing from the bucket and incompatible heads.
8. Reveal pages out of order, omit links, hide only the latest snapshot,
   and supply unrelated higher txids/generations. Only exact ancestry or
   a validated selected-chain generation above the stamp proves the floor.
9. Stream pages continuously, each within five seconds but total over
   15 s; discovery must cancel. Cover hung GETs, permit queues, all nine
   delays and jitter, ten-attempt exhaustion, and replay returning to
   discovery without resetting either budget. Exercise caller cancellation,
   move hint expiry, delete waits and lapse during backoff.
10. Exact-floor sidecar and matching file with LIST behind or empty succeeds
    for a complete requirement. Wrong checksum, pending WAL, stale sidecar
    and unrelated listed branch fail; lower-bound sidecars do not certify
    completeness after a crash.
11. Roll new readers through an old-node cluster: v1 writes until explicit
    enablement, old-node capability refusal, pre-enable rollback, and
    post-enable rollback only to v2-capable binaries. Test accidentally
    routed old nodes, old CLI output and delayed v1 conditional releases.
12. V1, v2, owned and unowned `wasmkeep-owner moved` tombstones coexist.
    Migration and decoding preserve their separate meanings; neither
    conversion nor rollback opens a tombstoned source.
13. Restore past content as a new segment, hide it on hand-over, and require
    the replacement rather than the source position. Exercise whole-app
    restore with no owners, seeded generations, removed actors, invalidated
    local files and the preserved app-format setting.
14. Crash after writing in the lower-bound phase, and while parked. Exercise
    strong-LIST recovery, refusal on known inconsistent storage and the
    documented limitation under undetected crash-time LIST lag. Do not
    claim safety for that unsupported case.
15. Lose the demotion reply, then lapse, park or drop. Land demotion before
    and after an old-version release, including a lost release reply.
    Resolution must eventually release on a responsive store, never retry
    the wrong version forever, revive an ended hold or upload prematurely.
16. Lapse with local H retained, let another node take over on the supported
    crash path and release Y at a higher ownership generation, then
    reacquire in the original process. Retire stale H without requiring
    comparable branches; retain it for same-generation, absent, corrupt or
    failed reads and require transfer for a local successor.
17. Pause `end_all` between registry enumeration/drain and cell freeze while
    acquiring the same key. Also pause `take` between emptying and unregister,
    and acquisition between GET, preparation and PUT. The acquirer must see
    an ending entry or the frozen head; a lapse ends prepared reservations.
    Exercise periodic snapshots due immediately after restore: no upload
    before demotion and no head advance from their validity check.
18. On an enabled app, perform in-place restore with `--deploy backup` from
    before enablement, then app delete with `--recreate`. The format key
    survives both; fresh ownership records are v2. Refuse incapable starts
    and capable-but-disabled v2 readers, without a v1 overwrite.
19. Under permanent lag, hammer requests, alarms, moves, deletes and expiry
    through repeated refusals and actor eviction. Assert coalescing, the
    cooldown schedule, at most ten listings and one generation increment
    per admitted cycle per node, bounded resolver work, and unchanged idle
    origin. Reveal the floor later: activation or eligible expiry succeeds;
    neither repeated rewrites nor stale local evidence prevent progress.

Assert safety over at least 200 seeds: every protected activation covers
its required acknowledged state; no history is written before verification;
no stale release changes a successor; no acknowledgement or activation uses
an ended hold. Assert liveness separately when evidence becomes visible
within budget and after collection on strong storage. Count each scheduled
race, delay, refusal and crash so a case cannot pass without exercising it.

Broken variants must fail: drop local H on re-acquire; let resolve release
only F; stamp an inherited floor with each new generation; compare against
the head generation instead of the stamp; acknowledge outside the cell;
keep complete-inherited after permitting uploads; unconditionally demote
on acquire; reject a valid exact-floor sidecar; accept a key-only witness;
let v1 writers run after enablement; confuse tombstones with free records;
restart a deadline on every page or retry; map visibility refusal to Broken;
report a refused move as complete; retry a pre-demotion version forever;
keep superseded local evidence after remote takeover; drain cells out of
visibility before freezing; erase the format key on restore/recreate;
reset cooldown on every request; or refresh idle age on a refused cycle.

### Real-code tests

- Codec fixtures for both versions, phases, stamps, strict field counts,
  unknown headers and both tombstones. Run the actual old decoder against
  v2, and exercise old routing/expiry failure and new dual-read paths.
- Ownership/hold tests for every release site, the registration-to-spawn
  gap, pending and parked head transfer, read-back and resolver versions,
  and the inherited-phase transition. Release retries remain identical.
- Planner/replay tests with real LTX bytes for exact checksums, descendant
  snapshots, pruned history, resets, corrupt objects and sidecar proofs.
- Fault-store tests with paused time for listing streams, proof GETs,
  cancellation, permits and upload completion races. Verify unchanged
  original files before a successful replay and no premature history write.
- Server tests for 503 visibility refusal, eventual successful activation,
  refused/retried moves, cancelled jobs and delete/erase after restore.
  Check the replay size allowance and exclusions separately from discovery.
- Mixed-version enablement/rollback, migration and ADR 0033 replacement
  tests. After deterministic checks, run a real-bucket hand-over check;
  passing it establishes only the tested case, not general consistency.

## Alternatives

- **Only diagnose the bucket or wait twice.** Useful operational checks,
  but neither prevents activation from a complete-looking stale chain.
- **Require the original floor forever.** Collection and erase legitimately
  remove it; this would break failover even on strongly consistent storage.
- **Publish lower bounds on every lapse.** Avoids claiming an exact frozen
  head but gives up protection on recoverable lapse releases. The shared
  hold-cell critical section preserves it without blocking the lease task.
- **Keep completeness only in memory.** A crash during restore loses it;
  persisting it without the pre-write phase change mislabels later crashes.
- **Publish before every durable reply.** Could protect crash recovery too,
  but adds a write on every acknowledgement; that is a different protocol.
- **Append fields to v1.** Older readers silently ignore and erase them.

## Consequences

Complete hand-overs gain a verifiable floor. Temporary invisibility costs
bounded discovery waits; persistent missing evidence refuses activation.
Generation stamps let collection and erase retain their existing bounds.
The authoritative head and release metadata must survive every local hold
transition. Writing from a complete inherited hold costs one additional
ownership PUT before its first upload, with no extra PUT per segment or
just for parking. Format enablement is an explicit operational transition.
Crash recovery after new writes still relies on the supported storage
contract; the discovery bound is not a total restore-time promise.

## Exact amendments to earlier records

Applied to the headers of 0007, 0020 and 0024 on acceptance
(2026-11-17); see "As built".

- **0007, metadata and Restore:** add the stamped floor and three proof
  routes before activation; bound complete listing streams and discovery
  retries separately from replay, retaining one consumed replay allowance.
  Separate complete hand-over safety from strong-LIST crash recovery.
- **0012, collection and erase:** allow a validated selected-chain object
  above the stamp to prove the inherited floor after pruning; never keep
  erased history solely for this check. Prohibit collection before restore
  verification and retain the reset segment/snapshot rule.
- **0020, sections 1–4:** put acknowledgement and ending in the hold cell;
  propagate local heads through pending/in-flight releases and acquisition;
  distinguish fixed release retries from resolver read-back; add inherited
  completeness and the conditional pre-upload phase change.
- **0020, section 6:** add the capability-gated app-format transition,
  dual readers, old routing/expiry failures, explicit rollback boundary and
  migration-tombstone carve-out; preserve generation seeding.
- **0028, idle expiry and app deletion:** preserve effective idle age on
  refusal, gate expiry's conditional acquisition and retain the separate
  format key across deletion/recreation; share per-actor retry cooldowns.
- **0024, parking and wake:** retain the head before `after_wake` discards
  a parked entry and transfer it through unresolved wake resolution.
- **0033, sections 5–6:** whole-app restore has no owner records and starts
  with unknown evidence, preserving the format setting and start barriers;
  single-actor replacement verifies live state and records its new head.
- **0034, barriers and release:** acknowledgements advance the hold cell;
  complete-inherited changes durably before uploads; captured batches and
  plain replies never count as the recorded durable head.

## Open questions

Follow-ups from the review of the build:

- **S3 (resolved in 0.5.1):** an idle expiry's acquisition writes the
  idle origin it judged, a hold never activated releases it unchanged,
  and the expiry pass reads the carried `idle-since` of the records this
  node refused: a refused actor still ages into expiry.
- **S4 (resolved in 0.5.1):** a local file at the floor with a
  conflicting checksum is set aside and the history downloaded; the
  replay judges the bucket.
- **S5 (not applicable as built):** no `serve` flag changes the heartbeat
  or lease timings; every node and the CLI use the same constants
  (`DEFAULT_HEARTBEAT`, `LeaseConfig::default_safe`, `LATE_WRITES`). Should
  the timings become configurable, `--enable` must read them from the
  cluster.
- **S6 (resolved in 0.5.1):** a missing link is listed again within the
  discovery bound; still missing then, it is reported as an incomplete
  history (500).
- **S7:** the simulations test the shared protocol code and a model of
  the server's ordering, not the server's own ordering. 2026-11-17:
  `sim/tests/madsim_server_order.rs` runs hand-overs in the server's
  order over a lagging LIST; no violation at 2,000 seeds. Its order is
  copied by hand: sharing the gate from the cluster crate would close it.

- **The exact-floor sidecar proof (proof 3, case 10)** stays unbuilt:
  it would let an actor activate from its own local file while the
  listing is behind, a liveness shortcut. Safety does not depend on it.
  Nor is the structural route beyond the snapshot stop (a listed floor
  the base snapshot links back to through listed segments) a safety gap
  for want of a checksum at the floor, under these preconditions:
  - **Segments are bound per key.** A segment key names its
    generation, transaction and parent generation; only that
    generation's holder writes it, created once (`If-None-Match`), and
    its bytes carry the writer's own pre- and post-apply checksums,
    which every replay checks. Create-only holds per key, not per
    position: the planner refuses a listing with two segment keys at one
    (generation, txid), so a listed segment at the floor's identity is
    the one object the floor names.
  - **Snapshots are not.** A periodic, after-delete or after-restore
    snapshot is labelled at the head it holds (`self.head`, an older
    generation's position), so a later holder may write a snapshot key
    in an earlier generation. Its content is bound by the checksum its
    writer verified at that head (one at the same key is the same
    position, the same database), and the replay of the selected chain
    starts from it. The route's links are segments only; the floor
    itself may be a listed segment or snapshot key, each bound to the
    checksum its writer verified there.
  - **The store enforces conditional create**, as `cluster diagnose`
    checks: no key is replaced once written.
  - **Nothing is restored into the prefix from another lineage.**
    Whole-app restore removes every ownership record and seeds
    generations above the restored keys; single-actor restore writes a
    new reset on the live head.

  Generations never restart for an actor whose record remains: deletes,
  idle expiry and `delete-self` write a reset at the next transaction and
  keep the record. Only `app delete` then `--recreate` removes both the
  record and the history, and generations may start again from 1; no
  floor survives it (the record is gone, and local evidence is dropped
  on an absent record).

- **Case 10 scoped (2026-11-17): liveness, not built.** The structural
  route indeed compares no checksum at the floor, but a local file is used
  only after the listing proves the floor and only at the plan's head,
  so proof 3 would add no comparison there; the route rests on the
  preconditions above, each checked in code. 2026-11-18, after a second
  review: the residual is that after `linked_back` a base snapshot with
  no segments above it, or a local file at the head, is trusted on its
  label alone. What would break the route, none of it caught by proof 3
  or a floor checksum: a store without conditional create; a late PUT
  from a deleted app landing after `--recreate` (generations restart, so
  an old snapshot key can become a base; only the `LATE_WRITES` wait
  prevents it); two segments with one id (now refused by the planner).
  The last needs a hold restoring twice in its own generation, which the
  server does not do: `Replica::restore` runs only right after an
  acquisition (`ActorState::ensure_owner_with`); a failed restore
  releases the hold, and a replica that stops replicating is given up
  (`abandon`, the hold dropped), so the next event acquires again at a
  new generation.

Earlier questions:

- Measure the 15 s discovery bound and ten attempts under concurrent
  hand-overs before changing their defaults; neither is a consistency claim.
- Define how operators persist and clear a known bucket-consistency failure
  without treating an untested bucket as proven consistent.
- Decide separately whether crash recovery on LIST-lagging stores is a
  supported goal. It requires another acknowledgement/publication protocol.
