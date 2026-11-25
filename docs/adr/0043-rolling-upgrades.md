# 0043. Rolling upgrades of WasmKeep

- Status: accepted (2026-11-25), not built. Section 3 describes the floor
  record as `0.8.1` already reads it; everything else is to build.
- Milestone: `0.9.0` (see [Now](../../ROADMAP.md#now) in ROADMAP.md)
- Follows: ADR 0008 (deploys roll; this record is about the binary), ADR
  0011 (the cluster port), ADR 0020 (node leases, the heartbeat, and the
  [upgrade floor record](0020-one-lease-per-node.md#the-upgrade-floor-record)
  that `0.8.1` reads), ADR 0022 (the app in every hop), ADR 0035 (a
  per-app format setting and its gate), ADR 0038 (capability words, the
  sender on every hop, "an offline node is not seen by a live-node
  gate"), ADR 0041 (the placed mark)
- Amends, when built: `docs/cluster.md`, "Upgrading WasmKeep" ("stop
  every node, upgrade them all, start them all" stops being the default
  from `0.8.1` to `0.9.x`); `docs/releasing.md`, "The mixed-version
  check" (a gate before a patch tag today; it becomes the proof of a
  minor upgrade); ADR 0020, "The upgrade floor record" (it names
  `finalize` as the floor's only writer and says the floor is not read
  after the start; section 3 adds a second writer, a starting node of a
  fresh app, and a read by a serving node of `0.9` and later)

## Context

Until now a cluster runs one version. The rule in `docs/cluster.md` is to
stop every node, upgrade them all and start them all, unless a release's
upgrading page says otherwise; two releases said otherwise (0.4.0 to
0.4.1 and 0.4.1 to 0.4.2, which changed no format the previous one
reads).

The pieces of a rolling upgrade exist, built one feature at a time:

- **Capability words.** A node's heartbeat (`nodes/<id>`, header
  `wasmkeep-node 3`) carries a line of words (`take`, `front`, `handoff`,
  `ends`, `flush`, `rate_limits`, `restore`, `actor-restore`,
  `recorded-head`, `multi-app`, `assets`, `blobs`, `place`). A reader
  ignores words it does not know (`NodeInfo::words`, `membership.rs`).
- **Writers that read them.** A deploy refuses a manifest a live node of
  the app could not read (`check_readers` and `check_imports`,
  `apps/readers.rs`); `app delete`, a secrets set with a `_front` entry,
  an actor restore and a whole-app restore have their own checks.
- **A per-app format setting.** `formats/<tenant>/<app>/recorded-head`
  (ADR 0035): one way, exact body, anything else refuses service.
- **A mixed-version check.** `scripts/mixed-version.sh` moves three nodes
  from the previous release to the checkout one at a time and one back.
  Since `0.8.1` it is a gate before a patch tag
  ([releasing](../releasing.md#the-mixed-version-check)). It does not
  yet prove anything a release may promise about a minor upgrade.
- **A floor record that `0.8.1` reads.** A node of `0.8.1` reads
  `formats/<tenant>/<app>/floor` at start and stops if it names a later
  minor version. Nothing writes it yet (section 3).

What is missing:

1. No statement of what is promised, between which versions.
2. No rule that says which new format needs a gate. Each feature decided
   for itself.
3. A live-node gate does not see a node that was stopped when the gate
   opened (ADR 0038, section 10). Such a node can start later into
   formats it cannot read.
4. Nothing stops a node from starting next to one two versions older.
5. No boundary after which going back is unsupported.

### Known problems, from 0.8.1

`0.8.1` documents no exception: its upgrading page says to stop every
node. Three known problems, each of which this work must solve or
measure before a page says "one node at a time".

1. **The window after the last node.** `0.8.0` to `0.8.1` was run one
   node at a time on a three-node cluster with a real bucket, under a
   held room load. The run failed on the window measured after the
   upgrade: after a node's restart, a room's edges were no longer
   spread evenly over the nodes, and the run's limits for that window
   were missed. A rolling upgrade restarts
   every node, so this is the upgrade's own problem and not a side
   effect: section 8's cluster run keeps that window, and passes only
   when the load settles within its limits after the last node.
2. **`cluster status` was slow.** In the same run it took 7 to 47 s,
   with about 140 `released` node leases under the app's `nodes/`
   prefix. An operator runs it after every step of an upgrade, and
   `cluster upgrade status` and `finalize` (section 4) read the same
   objects. Their time with that many leftover leases is measured, and
   bounded or documented with `cluster forget` as the remedy, before
   the upgrading page is written.
3. **`0.8.0` beside `0.8.1`, under a bucket that serves stale reads.**
   A node of `0.8.0` starting over a created fence can, in a narrow
   case, have the record it holds freed by a `0.8.1` reader until that
   reader sees a body dated past the fence's window: the node's next
   lease renewal, or the one after
   ([ADR 0020](0020-one-lease-per-node.md#nodes-of-080-beside-this-version)).
   `0.9` starts from `0.8.1` on every node, so that pair never meets a
   `0.9` node; it is one more reason the first supported pair is
   `0.8.1` to `0.9.x` and not `0.8.x`.

### Read in the code

Read at `0.8.0`. `0.8.1` changes no bucket format, no heartbeat word
and no signature between nodes, and the files and names below were
checked again at the `v0.8.1` tag. These facts decide the rules below.
Each was read in the decoder and in its main caller unless marked.

**The heartbeat and the node lease.**

- An unknown first line decodes to nothing (`NodeInfo::decode`,
  `NodeLeaseObject::decode`). A node whose object does not decode is
  absent from every listing: no capability gate counts it, no peer
  fences it, and its actors are never taken over (`frees` is false for
  an object that does not decode).
- The lease's second line is read with `split_once(' ')`: `live <instance>`.
  A third word on that line would become part of the instance. `frees`
  takes a live object naming another instance as proof the record is
  free, so every node of 0.8 would take over the actors of a live node
  that wrote such a line.
- After the three lease lines, the payload's first four lines (id,
  address, version, app) are required. Later lines are lenient: unknown
  capability words are ignored, an unknown drain word reads as none, a
  malformed load line reads as no load, unknown `key=value` words on the
  load line are skipped, and lines after the load are ignored.
- The version line is stored unchecked and compared only as a string by
  `cluster status`. It is `<major>.<minor>.<patch>` on a tagged build and
  `<major>.<minor>.<patch>+<commit>` otherwise. No code parses it.

**What a 0.8 node does with a bucket object it cannot read.**

| Object | Running node | Node at start |
|---|---|---|
| Deploy manifest of an unknown number, or with an unknown line | Keeps the version it runs, reports not ready ("cannot apply the deployed version"), retries with back-off (`apps/follow.rs`) | `serve` exits (`app_ref.version(..)?`) |
| `current` with an unknown token | As above | `serve` exits (`first_deploy`) |
| Ownership record with an unknown header or line | The acquisition fails as `Corrupt`; the actor answers 500 on every node of that version; nothing is written or deleted | Same |
| Ownership record `wasmkeep-owner 2` while the app's setting is off | Retryable refusal, never overwritten with version 1 | Same |
| App format setting with any other body | Every acquisition of the app fails (`format_gate`, `lease_hold.rs`) | Same |
| Secrets object of another header | Old set kept in memory, node not ready ("cannot open the app's secrets") | `serve` exits |
| Assets index of another header or with an unknown line | As an unreadable manifest | `serve` exits |
| A sibling key under `formats/<tenant>/<app>/` | Ignored: nothing lists `formats/` or builds another key under it | Ignored |

**What is ignored without an error.** A key under `wal/`, `snapshot/`,
`alarms/` or a control family whose name does not parse is skipped by
every reader and never deleted by collection. A history prefix holding
only such keys plans as an empty actor. A change to a key's shape is
therefore the most dangerous kind: nothing fails.

**The hop.** Two proofs exist, both HMAC-SHA256 under the cluster secret
(`cluster/signing.rs`, `cluster/forwarding.rs`):

- A node's call (`x-wasmkeep-internal: v4 <ts> <nonce> <body hash> <mac>`)
  signs the literal `wasmkeep-call v4`, the timestamp, the nonce, the
  target node, the method, the path, the body's SHA-256, and seven
  headers when present: `x-wasmkeep-caller`, `-chain`, `-alarm-handoff`,
  `-take`, `-app`, `-upgrade`, `-placed`. No other header is signed.
- A forward's proof (`x-wasmkeep-admitted`) signs the same, and in place
  of the body hash a hash of **every header that is not hop-by-hop, not
  `host` or `expect`, and not in the `x-wasmkeep-` range**
  (`identity_headers`). The client's headers are in it.
- `x-wasmkeep-sender` is a header of its own, bound to the hop's MAC. A
  signed call and a forward with a proof always carry it (`sign_call`
  and `sign_admission`).

What a 0.8 receiver does with a header it does not know:

- **On a forward** (not a signed call), every `x-wasmkeep-*` header
  except the forwarded mark is removed before the guest
  (`strip_outside_headers`). An unknown one is dropped silently.
- **On a signed call**, only named headers are removed (`requests.rs`:
  the app, the actor address, the alarm hand-off, the take, and the hop
  proofs). **An unknown `x-wasmkeep-*` header reaches the guest.**
- **Any header outside the range** (`traceparent`, for example) reaches
  the guest on both, as a client's header does today, and on a forward
  it is inside the proof's hash.
- On every arriving request, the headers outside the range are counted
  as the client's against the head limits (`client_head`, `http.rs`).
- A hop without `x-wasmkeep-app` is refused (`wrong_app`). A signed hop
  without a sender is accepted unless the process serves several apps
  (`sender_checked`, `cluster/signing.rs`).

**Local files.** No layout version is recorded in the data directory. The
compiled-component cache is keyed by Wasmtime's compatibility hash, not
by WasmKeep's version; an entry it cannot load is recompiled. The node
file (`wasmkeep-node.toml`) **rejects unknown keys and tables**, so
`serve` exits on a key from a later version.

## Decision

### 1. What "supported" means

1. **Scope.** From the last patch of the previous minor version to any
   patch of this one, and between patches of one minor version in both
   directions. The first pair is `0.8.1` to `0.9.x`; a cluster on `0.8.0`
   installs `0.8.1` first. One node at a time, by the documented steps:
   drain, stop, install, start, wait for `serving`. Skipping a minor
   version is refused by the node (section 5), not merely unsupported.
2. **During the upgrade.** Every acknowledged write is kept. Alarms fire
   as they do on one version. The cluster is never stopped as a whole
   and nothing is migrated offline. Clients see what a rolling restart
   of one version shows and no more: retries for a draining node's
   actors, reconnects for its sockets.
3. **Mixed state.** Supported for the length of an upgrade. It is tested
   for the length of the mixed-version check and for a two-hour soak on
   a three-node cluster; the upgrading page says so and does not promise
   a cluster kept mixed for days.
4. **New formats stay off** until every node reads them. That covers
   what the CLI writes (deploy, secrets, forget, restore, backup, app
   format) as well as what nodes write.
5. **Going back.** Until the upgrade is finalized (section 4), any node
   may return to the previous version. After, no.
6. **Not included.**
   - An app's SDK versions (ADR 0008 covers application code).
   - A breaking change to the guest interface (its own page when it
     happens).
   - A single node: a restart is downtime.
   - Any pair of versions a release's upgrading page does not name.
   - The `1.x` guarantee, which is decided at `1.0`.

A patch release adds no bucket format, no capability word that gates a
format, and no header on a hop. That is what makes patches of one minor
version interchangeable in both directions.

### 2. The gate table

The rule that decides: **anything the previous minor version would
misread, refuse or silently skip needs a gate; anything it reads past
needs a test and no gate.** "Silently skip" is in the rule because of
the key families above.

A gate has two halves, and a writer needs both:

- **The floor** (section 3) is at or above the minor version that
  introduced the format. It covers the node that was stopped when the
  gate opened.
- **Every live node of the app announces the format's word.** It covers
  a node that started while the floor was being written, and a
  capability a node's configuration can withhold.

Formats that exist in `0.8.1` keep their live-word gate alone: `0.8.1`
announces every word, so the floor adds nothing for them.

**The first gated format is deploy manifest 8.** It carries per-class
keys, `apart-from` and one for spreading, that ask for the built-in
placement rules for classes with the app's own names. Today the rules
apply by class name only: `pusher` and `room` are kept apart
([ADR 0041, amendment](0041-placement-at-wake-up.md#amendment-2026-11-22-keeping-a-room-and-its-pusher-apart)),
and `edge` is spread over the nodes (ADR 0040, section 14). It has a
word of its own, `apart`. `0.9.0` needs one
format it really writes behind the gate, or the gate ships untested
(section 8, phase 3).

| Thing | Form in 0.8 | What a 0.8 node does with a newer form | Rule |
|---|---|---|---|
| Heartbeat and node lease, lines 1 to 3 | `wasmkeep-node 3`, `<state> <instance>`, `<written-at>` | Unknown header: the node is invisible to gates and never failed over. Extra word on line 2: its actors are taken over while it lives | **Frozen, byte grammar included.** It carries the gates, so it cannot be gated |
| Heartbeat payload, lines 1 to 4 | id, address, version, app | A missing line: the node is invisible | **Frozen** |
| Heartbeat payload, later lines | Cluster address, pins, drain, `secrets`, words, load | Unknown words and load keys ignored; lines after the load ignored | Extend by words, by load keys, and by appended lines. No gate; a test |
| Deploy manifest | `wasmkeep-manifest 1` to `7`, exact match, every line known | Refused as corrupt (table above) | A new line is a new number. Gate each number, as `check_readers` does for 3 to 7 |
| `current` | `<version> <seq> 3` | Refused as corrupt | Frozen until a gated change is needed |
| Ownership record | `wasmkeep-owner 1`, `2` | Refused as corrupt; version 2 must re-encode byte for byte | Gate, with a per-app setting as ADR 0035 |
| App format setting | `formats/<t>/<a>/recorded-head`, exact body | Any other body refuses the app | Gate. A new setting is a new key, never a new body |
| Per-actor lease marker | `wasmkeep-lease 2` at `leases/<class>/<name>` | Neither known form: the acquisition fails as corrupt | Kept (section 6) |
| History objects | `LTX1`, header flags 0; keys of three or two 16-hex parts | Content: the actor answers 500, nothing deleted. **Key shape: skipped silently** | Content: gate. **Key shape: frozen** |
| Alarm index | Key shape only (`<class>.t/...` tagged, legacy read) | **Key shape: skipped silently**: no delivery from that node, no protection from idle expiry | **Frozen.** A new form needs a gate and a release in which both are written |
| Sealed secrets | `wasmkeep-secrets 1`, also in the AEAD's associated data | Refused; node not ready | Gate. The `_front` scope is gated on `front` today |
| Assets index | `wasmkeep-assets 1`, every line known, re-encoded byte for byte | Refused as an unreadable manifest | Gate (`assets` today; a new line needs a new header and word) |
| File sweep record | `wasmkeep-blob-sweep 1` | Skipped on every pass, never aged out, its files never removed | Gate |
| File count | `wasmkeep-blob-usage 1` | Treated as no count; lines after the fourth ignored | Extend by appended lines. A test |
| App tombstone | Presence at `apps/<t>/<a>/deleted`; only `at` and `id` lines read | Still a tombstone | Extend by lines. A test |
| Restore fence, `restored` | Presence; `restored` compared byte for byte | Still a fence | Extend the journal freely; `restored`'s body is frozen |
| Actor-restore request | `wasmkeep-restore 1`, strict | The node writes a refusal and deletes the request | Gate (`actor-restore` today) |
| Drain and control intents | Empty bodies; key shape | **Key shape: skipped silently**, never expired | **Frozen** |
| Backup run | JSON, `"format":1` | CLI only: `verify` and `restore` refuse; unknown fields ignored | Not a cluster format. A newer CLI reads older runs; an older CLI refuses newer ones |
| Change feed state | Local files and a binding string | CLI only: a different binding refuses the poll | Not a cluster format |
| Node call | `wasmkeep-call v4` and seven signed headers | A changed signed set does not verify: the call is refused | **Frozen for `0.9`** (section 7) |
| Forward's proof | `wasmkeep-ingress-identity-v2`, the client's headers hashed | Does not verify: the forward is treated as a client's | **Frozen for `0.9`** |
| A new `x-wasmkeep-*` header | n/a | Dropped on a forward; **reaches the guest on a signed call** | Gate on a word, per receiver |
| A header outside the range | n/a | Reaches the guest; counted in the client's head | No gate; section 7 |
| Guest interface | `wasmkeep:actor@0.2.0` | A component importing a function the node lacks does not link: not ready on a running node, `serve` exits at start | Gate each new import on a word. Only `storage.flush`, `storage.delete-self-durable` and the files interface are detected today; `check_imports` must learn each new one |
| Node file | Known keys only | `serve` exits on an unknown key | A key new in N is documented as "remove before going back" |
| Data directory | No version marker | Opens the same files | A change to the local layout needs a marker first; none is planned |
| Component cache | Keyed by Wasmtime's hash | Miss, recompile | Nothing |

Rows marked "CLI only" were checked by searching the node's crates for
the keys; no node reads them. The rows for history content, the alarm
index, the restore fence and the control intents were read once and not
traced into every caller (see "Not settled").

**The table becomes a test.** One registry in `wasmkeep-cluster` lists
every header constant and key family with: the minor version that
introduced it, its word (or `frozen`, or `ignored-by-previous`), and the
decoder's test for the unknown case. A test fails when a `wasmkeep-<name>
<n>` literal exists in the crates without an entry. Several headers are
inline literals today (`wasmkeep-assets 1`, `wasmkeep-restore 1`,
`wasmkeep-app-deleted 1`, the backup's `"format":1`); they become
constants in the same change.

**One helper.** `gate(format)` answers "may this writer write this
format now", from the floor and the live nodes' words, and names the
nodes that block. Every writer uses it, the CLI included. It replaces
the per-word functions in `apps/readers.rs` (`nodes_without_front`,
`nodes_without_ends`, `nodes_without_flush` and the inline checks).

### 3. The floor record

The record and its `0.8.1` reader exist: they shipped in `0.8.1` and are
defined in
[ADR 0020, "The upgrade floor record"](0020-one-lease-per-node.md#the-upgrade-floor-record).
This section repeats that definition, because every later version must
write exactly what `0.8.1` reads, and adds the writer and the readers of
`0.9`. Where the two records differ, the `0.8.1` code decides.

- **Key:** `<prefix>formats/<tenant>/<app>/floor`, next to the
  recorded-head setting. Per app, because node ids, heartbeats and the
  existing setting are per app.
- **Body, exactly:** `wasmkeep-floor 1\n<major>.<minor>\n`, two decimal
  numbers with no sign and no leading zeros, each within 32 bits, for
  example `wasmkeep-floor 1\n0.9\n`. It means: no node below this minor
  version may serve this app.
- **Decoding is exact.** Absent, or a body that encodes back to the same
  bytes. Anything else is unreadable: another header, a bare `9`, a
  third number (`0.9.0`), `0.09`, a trailing space, CRLF, a missing or a
  second final newline.
- **Absent** reads as "no floor": `0.8`, the last version without one.
- **Unreadable fails closed.** A node does not start, and a writer does
  not write, with a message naming the key and its length.
- **Written by conditional writes only** (create when absent, replace
  only the version read), and only upward. Nothing lowers or deletes it.
- **Outside `state/` and `apps/`.** `app delete`, `deploy --recreate`,
  a whole-app restore, a backup and `cluster migrate` never list or
  touch `formats/`. So the floor survives a delete and recreate, and it
  is not carried by a backup (see "Consequences").

**What `0.8.1` does, as shipped.** In the tree at the `v0.8.1` tag:

| What | Where |
|---|---|
| The header `wasmkeep-floor 1`, the key and the body | `crates/wasmkeep-cluster/src/app_format.rs`, `floor::HEADER` (line 75), `floor::key` (114), `floor::encode` (124) |
| The exact decode, and the refused forms as a test | the same file, `floor::decode` (138), `the_floor_is_absent_exact_or_refused` (182) |
| The comparison: refused only when the floor is above the binary's minor version | the same file, `floor::allows` (166) |
| The binary's minor version: the first two numbers of its version, split at `.`, `-` or `+` | the same file, `Minor::of_version` (95) |
| One read, within one lease attempt; no answer is not a verdict | `crates/wasmkeep-server/src/cluster/identity.rs`, `read_floor` (296) |
| The first read: after the bucket check, before the first lease write | `identity.rs`, `check_floor` (335), called from `start_node_lease` in `cluster/node_lease_task.rs` (227) |
| The second read: once the first lease write has landed, before the node serves | `node_lease_task.rs`, `node_lease_loop` (86 to 135) |
| The readiness reason | `crates/wasmkeep-server/src/cluster.rs` (1599) |

In words:

1. **Before its first lease write** a `0.8.1` node reads the floor. A
   floor above `0.8`, or an unreadable body, stops the start: the node
   writes no lease, so it never holds or serves anything. The log names
   the floor and the binary's version ("this app's upgrade was finalized
   at 0.9: this binary is 0.8, older than the floor, and must not serve
   it; install 0.9 or later"), and the node is not ready with the reason
   `the app's upgrade floor is above this node's version, or
   unreadable`.
2. **A read with no answer proves nothing.** It is tried again every
   half heartbeat period, and the node neither starts nor refuses.
3. **Again once its first lease write has landed**, before it serves.
   Until that write the node has no live lease, so a writer that listed
   the live nodes could not see it. A refusal there writes the node's
   lease `released` (it held nothing: acquisitions wait for the start's
   verdict) and stops it the same way. Without an answer the node stays
   checking, renews its lease each period and reads again after each
   renewal.
4. **Never after that.** A `0.8.1` node that is already serving when a
   floor above it is written keeps serving until it stops.
5. **Only from above.** `0.8.1` compares the floor with its own minor
   version and nothing else: it has no check from below and no check of
   other nodes' versions.
6. **It writes nothing.** No command of `0.8.1` creates, changes or
   removes the record.

A `0.8.0` binary does not read the floor at all.

**One race stays open on the reader's side**, and the writer closes it.
A writer that lists the live nodes before a starting node's first lease
write lands, and writes the floor after that node's second read, finds
no such node and is not seen by it. So `finalize` lists again after
writing (section 4, step 5), and every gate has a second half that does
not depend on the floor (section 2).

**Readers.**

| Reader | When | On a floor above its own minor version |
|---|---|---|
| A node of `0.8.1` | Before its first lease write, and again once it has landed | Stops, naming both versions; after the first write, its lease is written `released` |
| A node of `0.8.1`, serving | Never | Keeps serving: `finalize` must not write the floor while it is live |
| A node of `0.9` and later | The same two reads | The same |
| A node of `0.9` and later, serving | At a listing that shows a live node of a newer minor version | Drains and stops |
| The CLI of `0.9` and later | Before any write to the app | Refuses: "this `wasmkeep` is older than the cluster" |
| A writer of a gated format | In `gate(format)` | n/a: it compares the floor with the format's version |

A serving node reads the floor only when it sees a newer live node, so
an idle cluster on one version adds no bucket request.

**A node also checks the floor from below**, from `0.9` on. A node of
minor version N refuses to start when the app has registered nodes and
the floor is below N-1: the previous upgrade was never finalized. With
an absent floor, that stops `0.10` and passes `0.9`.

**A fresh app is born finalized**, from `0.9` on. A node that finds no
floor and no other node registered for its app creates the floor at its
own minor version, by conditional create. Without this, every new
cluster would need a `finalize` before its first gated deploy. A
`0.8.1` node started into such an app then stops at its first read.

### 4. `wasmkeep cluster upgrade status|finalize`

Finalizing is an explicit operator step. Formats do not open by
themselves when the last node is upgraded: the operator says the upgrade
is done, and that sentence is the boundary for going back.

**`wasmkeep cluster upgrade status --bucket <url> [--app <tenant>/<app>] [--json]`**

Read-only. Per app:

- each registered node with its version and whether it is live;
- the floor;
- one state:
  - `one version`: every live node on the floor's minor version;
  - `mixed`: live nodes on two minor versions;
  - `finalize pending`: every live node on minor version N, floor below N;
  - `refused`: something the skew check refuses (section 5), named;
- what blocks a finalize, by node id.

Exit code 0 for `one version`, 3 for `mixed` and `finalize pending`, 1
for `refused` or an unreadable floor. `cluster status` shows the floor
and the state in one line and keeps its own exit codes.

**`wasmkeep cluster upgrade finalize --bucket <url> [--app <tenant>/<app>] --yes`**

For each app (all apps with registered nodes under the prefix, or the
one named):

1. Reads the floor. Already at the target: says so and goes on (the
   command is idempotent).
2. The target is the CLI's own minor version. Refused when a live node
   is on a newer one ("finalize with that version's `wasmkeep`").
3. Refused while any live node of the app is on an older minor version,
   naming each. Refused with no live node: there is nothing to finalize.
4. Writes the floor by conditional write. A lost race is read again.
5. Waits one start check (a lease's ttl plus grace, plus a heartbeat
   horizon) and lists again. A `0.8.1` node that was starting meanwhile
   has normally stopped itself by then, at its second read of the floor
   (section 3), and shows as `released`. An older node that is live at
   this listing passed that read before the floor was written: it is
   reported as an error naming it, with exit code 1. The floor stays:
   gated formats remain refused while that node is live (the second half
   of every gate), and the node must be stopped. This second listing is
   what closes the race section 3 leaves open; it is not optional.
6. Prints the registered nodes that are not live and still name an older
   version. They are not an error: such a node stops at its next start.

Without `--yes` it prints what it would do and exits 2. There is no
`--force` and no way back.

**What finalizing changes.** Before it, nodes and CLIs of version N
write only what N-1 reads, so the bucket holds nothing an N-1 node
cannot read and any node may go back. After it, writers may use the
formats N introduced, each still subject to its word. Going back is
then unsupported, and a `0.8.1` node that tries stops at its first
read of the floor.

**Per-app formats.** A format an app opts into (a manifest line, a
setting) is gated like any other: floor and word, and then written only
when the app asks. This is stricter than leaving such formats on their
word alone; it keeps one rule and makes "before finalize, nothing new is
in the bucket" true without exception.

### 5. The skew check

**The parse.** A version is `<major>.<minor>.<patch>`, three decimal
numbers, optionally followed by `+` or `-` and anything. Only major and
minor are compared. Two builds of one release at different commits are
the same version here (they are different strings to `cluster status`
today; that display stays).

**At start.** During the identity check, which already lists the nodes
and lasts a lease's ttl plus grace, a node of minor version N refuses to
start when a live node of its app:

- is on a minor version below N-1 or above N+1, or on another major
  version: "node `<id>` runs `<version>`; this version runs only beside
  `<N-1>` to `<N+1>`. Upgrade one minor version at a time";
- has a version line that does not parse: refused the same way, naming
  the line.

An object under `nodes/` that does not decode at all cannot be judged by
its fields. If it is rewritten between two listings of the check, a
node this version cannot read is running: the start is refused, as the
existing start guard does for a heartbeat from before node leases.

**While running.** A node never stops because another node started.
Seeing a live node outside its range, it logs an error once a minute
and `cluster status` warns; the newcomer is the one that refused. The
one exception is the floor (section 3): a node below the floor drains
and stops.

**The CLI.** A command that writes refuses when a live node of the app
is outside the CLI's range, with the same message. Read-only commands
(`cluster status`, `cluster upgrade status`, `actor where`) warn and
go on.

**What `0.8.1` does.** It has no skew check: at start it reads the
floor and nothing about other nodes' versions (section 3). Beside `0.9`
it is in range. Beside `0.10` it cannot happen in a cluster that
followed the rules: `0.10` does not start until the floor is `0.9`, and
a floor of `0.9` stops `0.8.1` at start. The parse above agrees with the
one `0.8.1` applies to its own version (`Minor::of_version`).

### 6. Removing old paths

The rule: **a version may drop what only versions before the previous
minor write or send. Anything the previous minor version still writes
or sends stays readable through this whole minor version.** It is
checkable in the previous minor's tree: a form is removable only when
that tree has no writer or sender for it. Each removal names the last
version that carries the path, on the upgrading page.

For `0.9.0`, against the `0.8` tree:

**The one-time migration of ADR 0022: removed.** Gone are `migrate.rs`,
its simulation and `wasmkeep cluster migrate`. It is offline and
one-time, so it is outside the mixed-version promise. Cut-off: a cluster older than
`0.4.0` goes through `0.8.x`.

- What stays is a small start check that recognises an unmigrated
  layout and says "migrate with 0.8.x first". `legacy_layout::start_check`
  (`crates/wasmkeep-cluster/src/legacy_layout.rs`) is called by every
  deploy (`check_layout`) and is the only part with a production caller
  outside the migration.
- Reading a migration tombstone at an ownership key stays too
  (`OwnerObject::decode`, with `legacy_layout::tombstone_target`).
  Migrated buckets keep their tombstones for good, so a tombstone must
  still decode as one, never as a record or an unknown version. It has
  no production caller: no node reads the layout from before `0.4.0`,
  and the start check reads a tenant's commit before it lists those keys.
- The upgrading page of `0.9` (`upgrading-to-0.9.md`, not written yet)
  must name this removal and its last carrier, `0.8.1`.

**The per-actor lease marker: not removed in `0.9.0`.** `mark_old_lease`
costs one GET and one PUT when an actor's first ownership record is
created. Dropping it was conditional on three checks: nothing else
depends on the marker's seed; the seed does not matter after an actor
is deleted and recreated; and a mixed-version case passes both ways (an
actor created without a marker taken by `0.8.1`, and the reverse). The
first check fails: a whole-app restore in place reads every marker and
ownership record to collect each actor's highest generation, deletes
the state, and writes one marker per actor as a generation floor
(`backup/restore.rs`, the journal's `seeds`). `seed_generation` then
starts the actor's next record above it. Without the marker's read, a
restored actor could reuse a generation its deleted history carried.
The marker is therefore a live format with a writer in `0.8.1`, and the
rule above keeps it. Removing the per-actor cost needs the restore's
generation floor to move elsewhere first; that is a record change,
gated, and not before `0.10`.

**Acceptance of older senders: removed, in part.** What `0.8.1`
provably sends, read in `sign_call`, `sign_hand_off` and
`sign_admission`: `x-wasmkeep-app` on every hop, and the sender on every signed call and on every forward
that carries a proof. So `0.9.0` may remove:

- the `Ok(None)` branch of `sender_checked` for a hop that carries a
  call signature or a forward's proof (a signed hop with no sender is
  then refused in every configuration, as it is today with several
  apps);
- the 32-digit identity form in `admission_received` ("previous
  uncharged identity format"): `sign_admission` writes 33 or 64 digits;
- the hello's tolerance of a 404 from nodes of `0.4.1` and before;
- heartbeat headers `wasmkeep-node 1` and `2` in `NodeInfo::decode`, and
  with them the start guard for heartbeats from before node leases. A
  leftover object in those formats then reads as "does not decode",
  which section 5 handles: it blocks a start only if it is rewritten.

What stays: a forward with no proof at all. `sign_admission` sends none
when the cluster secret is not loaded or the peer address is unknown,
and whether a serving `0.8.1` node can be in that state was not settled
by reading (see "Not settled"). Plain-HTTP `accept` mode is a
configuration, not a shim, and stays.

Each of these removals gets a mixed-version case with `0.8.1` on the
other side before it merges.

### 7. The wire rule for hops

**A `0.9` node signs and sends hops exactly as `0.8.1` does.** The
literal `wasmkeep-call v4`, the seven signed headers, the proof's key
derivation and its hash of the client's headers do not change in `0.9`.

Three rules follow from what the code does:

1. **Nothing new is added to the signed set.** A header added to the
   list in `signature` changes the MAC only when present, so it could be
   sent to nodes that announce a word, as the placed mark is. `0.9`
   needs none.
2. **A new `x-wasmkeep-*` header is sent only to a receiver that
   announces a word for it.** On a signed call a `0.8.1` receiver would
   pass it to the guest.
3. **A header outside the range may be added without a gate, on three
   conditions.**
   - It is in place before the forward's proof is computed, because the
     proof hashes it. Changed after, the proof fails and the owner
     treats the forward as a client's.
   - It is counted: the owner counts it in the client's head. A node
     adds it only when the head stays within the limits with it;
     otherwise the request goes on unchanged.
   - It is safe for a guest to see, since `0.8.1` passes it on.

   `traceparent` (ADR 0044) is the only such header in `0.9`.

A change to the signed message itself (a `v5`) is the general case:
verify both for one minor version, send the new one only after the
floor says every node verifies it. Not needed in `0.9`.

### 8. How the mixed-version check becomes the proof

The script is a gate before a patch tag since `0.8.1`. In `0.9.0` it is
the proof of this record, with `OLD_VERSION=v0.8.1` (the published
asset, its SHA-256 pinned). Every phase counts in the exit status.

1. **Up, under load.** After every step, through every node: the
   counter keeps every acknowledged write, `actors.call` works between
   every pair of nodes, alarms armed before the step fire once, secrets
   are readable, room clients hold 1..N.
2. **Back and forward before finalizing.** One node returns to `0.8.1`
   and goes up again. Same checks.
3. **The gate.** With one `0.8.1` node live, a deploy that needs the
   new manifest number is refused and names that node. With every node
   on `0.9.0` and before `finalize`, it is still refused and names the
   floor. After `finalize`, it is accepted. This needs a format `0.9.0`
   really writes; without one the phase proves nothing.
4. **The returning old node.** After the gated manifest is deployed, a
   `0.8.1` binary is started. It must stop at its first read of the
   floor, before any lease write, with the message of section 3, and
   the counter continues on the others. Then the floor
   read is skipped by starting `0.8.0` instead: it must exit on the
   manifest it cannot read, and serve nothing.
5. **The race.** A `0.8.1` node is started and `finalize` is run before
   its first lease write. What must hold is that the old node never
   serves and the gated deploy is never accepted while it is live. Two
   outcomes pass. Usually the node stops itself at its second read and
   leaves a `released` lease, and `finalize` exits 0. If it passed that
   read first, `finalize` exits 1 naming it, the gated deploy stays
   refused while it lives, and is accepted once it is stopped. The
   second outcome cannot be forced with the real binary, so it also has
   a cluster test that writes the old node's lease directly.
6. **The skew check.** A `0.9.0` node is started beside a live
   heartbeat written with version `0.7.0`: refused, naming it.
7. **The wire.** With tracing on at the new nodes, calls and forwards
   between versions pass in both directions, and a guest on the old
   node receives `traceparent` as an ordinary header.
8. **CLI skew.** The old CLI against the finalized cluster and the new
   CLI against the mixed one, for `deploy`, `secrets set`, `cluster
   status`, `cluster drain` and `backup create`. Each either works or
   is refused with a message that names the version; none writes a form
   the other side misreads.

Where it runs: on a workstation before a tag, and as one nightly job on
`main`. Never on a pull request.

The second half of the proof is a three-node cluster on a real bucket:
`0.8.1` to `0.9.0` one node at a time under a held room load (the load
of [the 0.8.0 measurement](../benchmarks.md#a-big-room-on-three-nodes-080),
with default placement), with no gap and no repeat for any client and
each node serving within 60 s of its stop; the window after the last
node within the same limits as before the first (the window the `0.8.1`
run missed, see "Known problems"); two hours in the mixed state with no
acknowledged write lost; then `finalize` and a gated deploy accepted.
`cluster status` and `cluster upgrade status` are timed at every step.

**What the upgrading page may say.** `upgrading-to-0.9.md` names one
pair, `0.8.1` to `0.9.0`, and only once both halves pass. A cluster on
`0.8.0` installs `0.8.1` first, every node stopped, as that release's
page says.

## Alternatives rejected

- **Formats open by themselves** once every live node announces the
  word, as per-app formats do today. Simpler, but the stopped-node hole
  stays and there is no boundary for going back.
- **One floor per bucket prefix.** One command for every app, but nodes,
  heartbeats and the existing setting are per app, and two apps on one
  prefix may be upgraded on different days.
- **The floor inside the heartbeat.** The heartbeat is per node and
  dies with it; the floor must outlive every node.
- **Versions instead of words.** With a floor, "every live node is at
  least N" could replace a word per format. Words stay because a
  capability can be withheld by configuration (`place`) and because a
  version comparison of two untagged builds says little.
- **A new node-lease header for richer leases.** Not possible without a
  stop: an unknown header makes a node invisible and never failed over.

## Consequences

- An upgrade gains one command. Skipping `finalize` costs nothing until
  the next minor version, whose nodes refuse to start below the floor.
- **A new minor version's features are unavailable until `finalize`.**
  The upgrading page says so in its first paragraph.
- **The node file is part of going back.** A key added in `0.9` stops a
  `0.8.1` binary. The upgrading page lists the keys to remove first.
- **A backup does not carry the floor**, nor the recorded-head setting.
  A whole-app restore in place keeps the target's floor. A restore into
  another prefix creates state with no floor, and a `0.8.1` node could
  start there on formats it refuses; the restore command should write
  the floor of the version that restores. That is a change to the
  restore, not designed here.
- `app delete` and `deploy --recreate` keep the floor. An app recreated
  after a finalize stays finalized.
- The frozen rows are a constraint on later records: the node lease's
  three lines, the payload's first four, the key shapes of history,
  alarms and intents. A change there needs a release in which both
  forms are written, or a stop.
- Two GETs per node start (before and after the first lease write) and
  one per CLI write. None while a cluster on one version runs.

## Tests

- `floor`: the decode's test exists since `0.8.1`. Added: the writer
  (create when absent, replace only the version read, never downward),
  and one test that the bytes `0.9` writes are the bytes the `0.8.1`
  decoder accepts, kept as a fixture so a later change to the encoder
  fails it.
- The registry test of section 2.
- `gate`: every pair of floor and live words, including a live node
  that does not announce the word above the floor.
- Start: a node of N beside N-2, N-1, N, N+1, N+2; beside an unparseable
  version; beside an object that does not decode, static and rewritten;
  below and above the floor; with a floor below N-1.
- `finalize`: idempotent; refused for each precondition; the race of
  section 8, phase 5, as a cluster test.
- No simulation: the lease protocol does not change. A simulation of
  two protocol versions is needed only when a lease or replication
  message changes between minor versions.

## Not settled

1. **The window after an upgrade** (Context, 1). Why a room's edges
   stay uneven after a node's restart, and whether placement, the
   upgrade steps or the limits change, is not decided here. The proof
   of section 8 does not pass until it is.
2. **The cost of `cluster status` and of the two new commands with many
   released node leases** (Context, 2): to measure, then bound or
   document.
3. **A forward with no proof** (section 6): whether a serving `0.8.1`
   node sends one. Until read or tested, that branch stays.
4. **A restore into another prefix** and the floor ("Consequences").
5. **Rows of the gate table read once** (history content, alarm index,
   restore fence, control intents) and four items on the wire read only
   in comments: the take call, the socket hand-off relay, the placed
   mark sent to a node without `place`, and the apart refusal's reason.
   The mixed-version check exercises each; none was traced end to end
   for this record.
6. **What a `0.8.1` CLI does against a finalized cluster** beyond
   failing on a manifest it cannot parse. It has no floor check; phase 8
   is the test.
7. **A `0.8.1` node that cannot read the floor after its first lease
   write** stays live and checking, without serving, for as long as the
   read has no answer. `finalize` sees it as a live older node and
   refuses, which is the safe side; whether `cluster upgrade status`
   should name that state apart was not designed.
8. **"No other node registered" is not proof of a fresh app** (section
   3, "born finalized"). An app with state whose node leases were all
   removed with `cluster forget` looks the same to a starting `0.9`
   node, which would then write the floor and end the way back without
   a `finalize`. The condition probably has to include "no deploy and
   no state under the app"; to settle when the floor's writer is built.
