# 0043. Rolling upgrades of WasmKeep

- Status: accepted (2026-11-25), items 3 and 4 built: the skew check at start
  (section 5), the gate table as a test and the one gate helper (section
  2), the floor's writer and the born-finalized rule (section 3), and
  `cluster upgrade status|finalize` (section 4). What that change decided,
  and what it left to build, is under
  ["Built: the upgrade mechanics"](#built-the-upgrade-mechanics). The
  first gated format, deploy manifest 8 with the word `apart`, is under
  ["Built: the first gated format"](#built-the-first-gated-format).
  Section 6's removals of older senders, and a second gated format, the
  mark of `cluster forget`, are under
  ["Built: the removals, and the mark of `cluster forget`"](#built-the-removals-and-the-mark-of-cluster-forget).
  Section 8 is still to build.
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
  `recorded-head`, `multi-app`, `assets`, `blobs`, `place`, and
  `apart` from `0.9`; `traces`, from a `0.9` node with tracing on, gates
  nothing, ADR 0044). A reader
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
keys, `apart-from` and `spread`, that ask for the built-in
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
| The mark of `cluster forget` at `nodes/<id>` | n/a (`wasmkeep-node-forgotten 1`, from `0.9`) | Not a heartbeat: skipped by every listing. A node of `0.8.1` starting under that id would write its lease over it | Gate on the floor at `0.9`; no word (the node it matters to is not live) |
| Heartbeat payload, later lines | Cluster address, pins, drain, `secrets`, words, load | Unknown words and load keys ignored; lines after the load ignored | Extend by words, by load keys, and by appended lines. No gate; a test |
| Deploy manifest | `wasmkeep-manifest 1` to `7`, exact match, every line known | Refused as corrupt (table above) | A new line is a new number. Gate each number, as `check_readers` does for 3 to 7. `wasmkeep-manifest 8` (`0.9`, the `apart` and `spread` lines): gated on the floor at `0.9` and on the word `apart`, and written only for a version that has such a line |
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
| Backup run | JSON, `"format":1`, or `2` for a run that carries an app's floor | CLI only: `verify` and `restore` refuse; unknown fields ignored | Not a cluster format. A newer CLI reads older runs; an older CLI refuses newer ones. What an older CLI must not drop in silence gets a new number, written only for a run that has it (section 3, "A restore") |
| Change feed state | Local files and a binding string | CLI only: a different binding refuses the poll | Not a cluster format |
| Node call | `wasmkeep-call v4` and seven signed headers | A changed signed set does not verify: the call is refused | **Frozen for `0.9`** (section 7) |
| Sender of a hop | `wasmkeep-sender v1`, bound to the hop's MAC | Does not verify: refused where the sender is checked | **Frozen for `0.9`** |
| Bodies of a take call and of an alarm hand-off | `wasmkeep-take 1`, `wasmkeep-alarm 1`, every line known | Any other body is refused | **Frozen for `0.9`** |
| Per-actor lease from before node leases | `wasmkeep-lease 1` | Read once to seed an actor's first record; never written | Read only (section 6) |
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

**The table becomes a test.** One registry in `wasmkeep-cluster`
(`upgrade::FORMATS`) lists every header constant and key family with: the
minor version that introduced it (the first release whose tree holds it),
its rule (frozen, extended, gated on words, read only, or the CLI's own),
and the decoder's test for the unknown case. The test
`every_format_constant_has_a_row_in_the_gate_table` scans the crates'
shipped sources and fails when a `wasmkeep-<name> <n>` literal has no
row, when a row names a test that does not exist, or when a gate names a
word no node announces. The headers that were inline literals
(`wasmkeep-assets 1`, `wasmkeep-restore 1`, `wasmkeep-app-deleted 1`, the
backup's `"format":1`) are constants. The scan found four literals this
table did not have, added above: `wasmkeep-sender v1`, `wasmkeep-take 1`,
`wasmkeep-alarm 1` and `wasmkeep-lease 1`.

**One helper.** `upgrade::gate(format, floor, live nodes)` answers "may
this writer write this format now", from the floor and the live nodes'
words, and names the nodes that block; `upgrade::without(words, nodes)`
is the one test of "every node announces these words" under it. Writers
reach both through `apps::Readers`, which reads an app's floor and live
nodes once: the deploy and the rollback (the manifest's row), `app
delete` (the tombstone's row), `secrets` (the `_front` scope, among the
nodes that read secrets), a whole-app restore (every manifest it writes,
on the floor it leaves: section 3, "A restore"; and its fence's word
among the registered nodes), an actor restore and `app
format --enable` (registered nodes, offline ones included), and a
process that serves several apps (`multi-app`, over the whole prefix).
It replaced the per-word functions in `apps/readers.rs`
(`nodes_without_front`, `nodes_without_ends`, `nodes_without_flush` and
the inline checks). What a running node decides about one peer (`take`,
`handoff`, `place`) is routing, not a write, and stays where it was.

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
- **Outside `state/` and `apps/`.** `app delete`, `deploy --recreate`
  and `cluster migrate` never list or touch `formats/`. So the floor
  survives a delete and recreate. A backup reads it and a whole-app
  restore may raise it, never anything else ("A restore", below).

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

**A fresh app is born finalized**, from `0.9` on. The first writer into
an app that holds nothing creates the floor at its own minor version, by
conditional create. Without this, every new cluster would need a
`finalize` before its first gated deploy. A `0.8.1` node started into
such an app then stops at its first read.

An app never ran when it has no floor record, no registered node, no
tombstone and nothing under `state/<tenant>/<app>/`:

- **No registered node.** An object under `nodes/` that does not decode
  counts as one, since it cannot be shown to belong to another app. A
  starting node leaves its own earlier lease out only when that lease
  names its own minor version or a later one. A single node restarted in
  place from `0.8.1` on `0.9` is not the first node of a new app: it
  creates no floor, and restarted on `0.10` it is refused, the upgrade
  to `0.9` never having been finalized.
- "No node registered" alone is not proof: an app whose node leases were
  all removed with `cluster forget` still has its state, gets no floor,
  and keeps its way back until a `finalize`.
- Secrets set before an app's first deploy, and the uploads of a deploy
  that never finished, do not make it an app that ran.
- Any doubt writes nothing: a listing that fails, or a create that loses
  to another writer.

There are two such writers, because with `serve --app` a node waits for
the app's first deploy and so never sees an app without one:

- **The first deploy** of an app (no `current`), once it is accepted and
  before it writes anything of the app. A deploy that is refused or
  cancelled writes no floor. It must not shut the cluster's nodes out of
  the new app, and a new app has no node of its own to ask. So it reads
  the live nodes under the bucket prefix, of every app, and takes L,
  their lowest minor version:
  - no node live under the prefix: the floor is the minor version of the
    `wasmkeep` that deploys;
  - a live node whose version does not parse: no floor is written;
  - otherwise F is the lower of the CLI's version and L. F at or below
    `0.8` (what no record means) writes nothing; above it, the floor is
    created at F.
- **A starting node**, at its first read of the floor, by the same rule
  as the deploy: the lower of its own version and the lowest version
  live under the prefix, itself left out, and nothing when that is at
  or below `0.8` or a live version does not parse. So the first `0.9`
  node of a new app beside `0.8.1` nodes of other apps creates no floor
  either, and those nodes can still start into the app. With no other
  node live it creates the floor at its own version. For a node, a
  deploy does not make the app one that ran: the
  deploy may have written no floor (the rule above), and an app with a
  deploy, no state and no other registered node has nothing a node of
  the version before could lose. So the first node of an app whose
  deploy wrote no floor creates it, and no later node finds an app with
  registered nodes and no floor that was never upgraded. (Without this,
  a node of N+1 starting there would be told its app was "never
  finalized".) What an operator gives up: for an app that has a deploy,
  no state, and whose node leases were all forgotten, the first node of
  the new version ends the way back.

**Both writers list the nodes again after the create**, as `finalize`
does after its write. A node of the app below the new floor that is live
then (or whose lease is stale without a release or a fence) started
between the listing and the create and never saw the floor. The floor
stays, and such a node cannot come back on its old version. The deploy
fails naming the node, having written nothing else; the starting node
does not start, naming it; both say to upgrade it. No wait is needed before
this listing: a node whose first lease write had not landed reads the
floor again once it has, and stops itself.

The deploy is a writer this record did not have when it was accepted.

**A restore.** Decided on 2026-11-25; it closes "Not settled" 4. The
whole-app restore was the one writer of `current` and of manifests
outside the gate, and a backup did not carry the floor.

- **A backup carries the floor.** `backup create` reads the app's floor
  record after its deploy (the floor is only raised, so what it reads is
  never below what the manifests it just read needed) and stores it in
  the app's record of the run, `"floor":"0.9"`, under the run's digests.
  An unreadable record fails the backup. An app without a record has no
  such field.
- **Only such a run is format 2.** A `wasmkeep` of `0.8` ignores fields
  it does not know and would restore the app without its floor. It
  refuses any format but 1 ("unknown or inconsistent backup format"),
  so a run in which an app carries a floor says 2. `0.8.1` then refuses
  to verify or restore it, and also to `create` into an archive whose
  latest run is one or to `prune` an archive that keeps one, since both
  load those runs. A run without a floor stays format 1, and `0.8.1`
  reads it as before. A newer `wasmkeep` reads both, and refuses a run
  whose number and field disagree.
- **The restored app's floor is the higher of the target's and the
  backup's.** A floor is only ever raised, across a restore too: the
  state and the deploy in the backup were written under the backup's
  floor, and are not served below it. The restore raises the target's
  record to the backup's when that is higher (conditional create, or
  replace of the version read), and writes nothing when the target's is
  that already. So a restore in the prefix the backup came from changes
  nothing: its floor is the backup's or was raised since. This holds
  with `--deploy keep` too: the floor goes with the state.
- **Never above the `wasmkeep` that restores, and never below the
  backup's floor.** A `wasmkeep` older than the backup's floor is
  refused before it writes anything.
- **The floor is the app's own**, as everywhere else: the gate, the
  writer's check and `finalize` look at the app's nodes, and `finalize`
  raises one app's floor beside older nodes of another. So the restore
  raises the floor whatever the nodes of other apps under the target
  prefix run, and warns, naming those that are live and older: they
  keep serving their apps and cannot start into this one. (A process
  that serves several apps registers one node per app, so a heartbeat
  names one app.) The first version of this rule refused instead; that
  made one app's restore depend on another app's upgrade.
- **The app's own older nodes, before the write.** When the floor has
  to be raised, the restore first lists the app's registered nodes, as
  `finalize` does. An older one that is live or not stopped for sure (a
  lease neither released nor fenced for a ttl plus grace, or a
  heartbeat from before node leases) refuses the restore before any
  write, naming it: fence it, or forget the old heartbeat. Those
  stopped for sure are named in what the restore prints; they cannot
  come back on their version.
- **A backup without a floor** (made by `0.8`, or of an app never
  finalized) restores as before and writes none. If it holds a manifest
  in a format behind the floor (the record was missing when the run was
  made), the gate applies on the target's floor; without one, on the
  floor a first deploy would create, which the restore then creates, and
  only for a target app that is new (no deploy, no registered node). An
  app that is not new is finalized by `finalize`, not by a restore.
- **Every manifest the restore writes goes through the gate**: the
  current version's and each kept one's, by the same path as a deploy
  and a rollback (`AppRef::check_restore`, on `Readers::gate`), on the
  floor the restore leaves, with the writer's own check. With `--deploy
  keep` it writes none.
- **Order.** The whole check runs before the restore writes anything in
  the target (after its own check that no node of the app is live), and
  again under its lock. The floor is written after the fence, the quiet
  wait and the undo backup of a restore in place, and before any state
  is removed or copied. That step runs on every run, a resume in any
  later phase included: the write (which finds the floor when it is
  there), then the nodes listed again whenever the app has a floor above
  `0.8`, as after a first deploy's create, then the gate on the floor
  the target now has. An older node of the app that is live or not
  stopped for sure fails the restore there, and on every rerun until it
  is fenced: the floor stays. `--abort` leaves the floor, as a refused
  deploy leaves the one it created; it does not bring back a lower one
  for the app's own stopped nodes.
- **What it gives up.** A restore in place from a backup with a higher
  floor than the target's ends the target's way back without a
  `finalize`, with no node of the app live: the offline finalize of
  "Not settled" 10, for this one case. And an archive that holds a
  format 2 run needs `0.9` for every backup command: a `0.8.1` `create`
  refused there has already written its run's `started` object, which
  holds `prune` back for 24 hours, and a refused `0.8.1` `restore` has
  left its archive pin. Backup jobs are upgraded before the first `0.9`
  backup of a finalized app; `create` says when an archive crosses that
  line. An undo run crosses it too: an in-place restore of a format 1
  run into an app that has a floor writes a format 2 undo run.

Tests: `upgrade::tests` (the rule alone, and the write with its second
listing), the backup tests of `wasmkeep-cli` (the run's format, a format 8
app into an empty prefix, beside older nodes of another app, an older
node of the app not stopped for sure, `--deploy keep`, a backup without
a floor, the higher floor kept, a rerun's listing, the undo run's format,
and the floor written before the app at every failed write), and `tests/cluster/restore.rs` (a restored app on
which a node of `0.8` stops at the floor and a node of `0.9` serves).

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
  - `no live node`: nothing to compare;
  - `node below the floor`: a live node runs a version below the floor
    (see below);
  - `refused`: something the skew check refuses (section 5), named;
- what blocks a finalize, by node id.

Exit code 0 for `one version`, 3 for `mixed`, `finalize pending` and `no
live node`, 1 for `node below the floor`, `refused` or an unreadable
floor; over several apps, the worst. `cluster status` shows the floor
and the state in one line per app and keeps its own exit codes.

**`node below the floor`.** A node that gets no answer on its floor read
after its first lease write stays live and checking, without serving
(section 3). Its heartbeat says nothing of that: the payload has no
start-check state, and `0.8.1` cannot be changed. So from the bucket it
looks like any live node of its version. While the floor is not above
it, that is an older live node: the app is `mixed` and it blocks
`finalize`, which is the safe side. Once the floor is above it, the
state is `node below the floor`, which covers both ways to get there: a
node that passed its floor reads before the floor was written and
serves, and one that keeps checking a floor it cannot read. The line
names the node, says both, and points at the node's readiness answer,
which tells them apart. Either way the node is to be stopped.

**`wasmkeep cluster upgrade finalize --bucket <url> [--app <tenant>/<app>] --yes`**

For each app (all apps with registered nodes under the prefix, or the
one named):

1. Reads the floor. Already at the target: says so and goes on (the
   command is idempotent).
2. The target is the CLI's own minor version. Refused when a live node
   is on a newer one ("finalize with that version's `wasmkeep`").
3. Refused while any live node of the app is on an older minor version,
   naming each. Refused too while an older node's lease is stale but
   neither `released` nor `fenced` for longer than a lease's ttl plus
   grace after its last write: a stale lease is not proof of a stop
   (the node may be cut off or slow, and still act), so the message
   names `wasmkeep cluster fence <id>`. Refused with no live node: there
   is nothing to finalize.
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
6. Prints the registered nodes whose lease is released or fenced and
   still name an older version. They are not an error: such a node stops
   at its next start. An older lease that is stale without either is
   reported with the live ones, as an error.

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

As built, the check lists the nodes twice: beside the node id check,
and again once that check passed, right before the first lease write. A
start is not made longer by it, except when the first listing holds an
object that does not decode: the second listing then waits until a
lease's ttl plus grace after the first, the time such an object's writer
is given to show itself. An object written under `nodes/` between the
two listings that does not decode refuses the start too. The node's own
key is left to the node id check, and a heartbeat from before node
leases to the start guard.

**While running.** A node never stops because another node started.
Seeing a live node outside its range, it logs an error once a minute
and `cluster status` warns; the newcomer is the one that refused. The
one exception is the floor (section 3): a node below the floor drains
and stops. (`cluster status` warns; the log line and the drain are not
built yet.)

**The CLI.** A command that writes refuses when a live node of the app
is outside the CLI's range, with the same message. Read-only commands
(`cluster status`, `cluster upgrade status`, `actor where`) warn and
go on. As built, the commands that refuse are `deploy` and a rollback,
`secrets` writes, `app delete` without `--force` and `app format
--enable`; the same check refuses a `wasmkeep` older than the app's
floor.

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
when the cluster secret is not loaded or the peer address is unknown.
A `0.8.1` node that serves one app can be in the first state (it
forwards without waiting for the secret), so that form is one `0.8.1`
sends, and the rule keeps it. Plain-HTTP `accept` mode is a
configuration, not a shim, and stays.

As built, the first three are removed and the fourth is not: see
["Built: the removals, and the mark of `cluster forget`"](#built-the-removals-and-the-mark-of-cluster-forget).

Each of these removals gets a mixed-version case with `0.8.1` on the
other side before it merges. That was not done for the change that
removed them: it rests on the reading of the `v0.8.1` senders and on
unit tests of each refusal. The mixed-version check
(`scripts/mixed-version.sh`, section 8) sends every kind of hop between
the two versions and must pass with these removals before `0.9.0` is
tagged.

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
- **A backup carries the floor** (section 3, "A restore"), not the
  recorded-head setting (`app format --enable` is run again after a
  restore into another prefix). A whole-app restore leaves the higher of
  the target's floor and the backup's. A run that carries a floor is one
  a `wasmkeep` of `0.8` refuses, so backup jobs are upgraded first.
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

## Built: the upgrade mechanics

Built on 2026-11-25, as the third item of `0.9.0`. The code is in
`crates/wasmkeep-cluster/src/upgrade.rs` (the rules, with the version
that asks given as a value), `app_format.rs` (`floor::raise`, the floor's
only writer), `crates/wasmkeep-server/src/apps/readers.rs` (`Readers`),
`cluster/identity.rs` (the start checks) and
`crates/wasmkeep-cli/src/upgrade_cmd.rs`.

Decided while building:

1. **The born-finalized condition** (was "Not settled" 8): no floor, no
   registered node, no tombstone and nothing under `state/` for the app;
   written by the first deploy, at the lower of its version and the
   lowest live version under the prefix, and by a starting node, which
   ignores a deploy; both list the nodes again after the create (section
   3).
2. **The node that keeps checking its floor** (was "Not settled" 7) has
   no state of its own before the floor is above it, and is `node below
   the floor` after (section 4).
3. **`no live node`** is a state of `upgrade status`, with exit code 3.
4. **The second listing of the skew check** is made when the node id
   check ends and delays a start only for an object that does not decode
   (section 5).
5. **The workspace version is still `0.8.1`**, so on this branch a build
   is minor version `0.8`: it creates a fresh app's floor at `0.8` (what
   no record already means) and `finalize` targets `0.8`. Every rule
   takes the version as a value and is tested for `0.9` and later. The
   version bump of the release turns them on.

Not built by that change:

- **`0.x` to `1.0` cannot be a rolling upgrade under this rule.** A
  version runs only beside the minor versions next to it in its own
  major version, and the floor from below has no "previous minor" across
  a major version. `1.0` decides its own rule (section 1, "Not
  included").
- **No format was behind the floor.** Manifest 8 and the word `apart`
  (section 2) came with the next item, below. Until then the floor half
  of `gate` was tested with a made-up format; every format `0.8.1`
  writes passes it by construction.
- **A serving node's read of the floor** when it sees a newer live node,
  and its drain and stop below the floor (section 3, "Readers"), and the
  once-a-minute error for a live node outside its range (section 5).
- **Writes with no gated format** (`cluster drain`, `fence`,
  `actor delete` and `move`, `backup create`; `forget` too, which asks
  the gate only for its mark, below) do not run the writer's
  check; an actor restore checks its word, not the floor. (A whole-app
  restore went through the gate later: section 3, "A restore".)
  `secrets rotate-key` has none either, on purpose: it stays
  usable during an upgrade, and writes the format `0.8.1` reads.
- Sections 6 (removals), 7 (nothing to build: `0.9` sends as `0.8.1`)
  and 8 (the mixed-version check as proof).

## Built: the first gated format

Built on 2026-11-25, as the fourth item of `0.9.0`: deploy manifest 8,
behind the floor and the word `apart`. What the keys mean is in
[ADR 0041, amendment of 2026-11-25](0041-placement-at-wake-up.md#amendment-2026-11-25-apart-from-and-spread-for-an-apps-own-class-names).

1. **The spread key is named `spread`** (section 2 left it unnamed):
   `spread = true|false` per class, beside `apart-from = "<class>"|false`.
2. **The row.** `wasmkeep-manifest 8` in `upgrade::FORMATS`: since
   `0.9`, gated on `apart` and on the words of the numbers below it. It
   is the first row whose `since` is above what no floor means, so the
   first for which `gate` checks the floor and the live nodes' versions.
3. **One writer, one path.** The deploy and the rollback reach it
   through `check_readers` and `Readers::gate`, as for 1 to 7; nothing
   checks the word on its own. A refusal names what blocks: the floor
   and `finalize`, the older nodes with their versions, or the nodes
   without `apart`.
4. **Only when asked.** A manifest is format 8 only when it has an
   `apart` or a `spread` line. Every other version keeps the number it
   had, so `0.8.1` nodes read it for the whole upgrade, and nothing
   new is in the bucket before the finalize (section 4, "Per-app
   formats").
5. **The word `apart` is announced by every node of `0.9`**, placement
   on or off: it says the node reads the format, not that it applies the
   rules.
6. **A fresh app's first deploy may be format 8.** The gate runs before
   the deploy is accepted, and the floor of a fresh app is created only
   after (section 3: a refused or cancelled deploy writes none). So a
   first deploy of an app without a floor record is checked on the floor
   it would create (`upgrade::deploy_floor`: its own version, or the
   lowest live version under the prefix). Once accepted it creates the
   floor, and a format behind the floor is checked again on the floor
   the app then has. An app that only looked new (state, a registered
   node) got none, and is refused there, before anything of it is
   written. Such a refusal does not send the operator to `finalize`,
   which needs a live node of the app: it names the older nodes under
   the prefix to upgrade, or says to deploy without the keys, start the
   nodes, finalize, and deploy again.
7. **The writer's version is a value.** `AppRef::as_version` lets a test
   deploy as a `wasmkeep` of `0.9` while the workspace version is still
   `0.8.1`, as `ClusterConfig::version` does for a node. No command sets
   it. With this build's own version (`0.8`), a deploy with the keys is
   refused at the floor: the release's version bump turns it on.

Tests: the gate's row and both halves (`upgrade::tests`,
`apps::tests::format_8_needs_the_floor_and_apart_on_every_live_node`),
and `tests/cluster/apart_from.rs`, the gated write section 8's proof
needs (phase 3): refused under the floor, beside an older node and
beside a node without the word, then accepted; and the first deploy of
a fresh app, of one with state, and of one beside older nodes of
another app.

## Built: the removals, and the mark of `cluster forget`

Built on 2026-11-25, as the fifth item of `0.9.0`.

### Older senders (section 6)

Before anything was removed, the senders were read in the tree at the
`v0.8.1` tag. `sign_hop` (`cluster/signing.rs`) is the only function
that writes a hop's signature, and it writes the sender right after, on
every path: `sign_call` (an actor call, a take call and an alarm
hand-off through `call_once`, the hello, and the relayed socket
hand-off through `sign_hand_off`) and `sign_admission` (a forward and a
WebSocket upgrade relay). The sender's value is always a valid header:
a node id is `[a-z0-9-]{1,64}`, checked at start. `sign_admission`
writes the client's address as a flag and 32 digits, or 64 for a
charged proof. What was read is recorded in
[ADR 0038, amendment of 2026-11-25](0038-several-apps-per-process.md#amendment-2026-11-25-the-older-forms-are-no-longer-accepted).

Removed:

1. **A signed hop without a sender is refused** in every configuration
   (`sender_checked`): a node call answers 503 and is never served as a
   client's request; a forward's proof without a sender is not a proof.
2. **The 32-digit client address** in a forward's proof is no longer
   read (`admission_received`).
3. **A 404 to a hello** is no longer taken as an answer: it is sent
   again as any failed hello, three rounds at most
   (`a_hello_answered_404_is_not_taken`).

Kept:

1. **A forward with no proof.** `0.8.1` sends one from a single-app
   process whose secret is not loaded, or when the client's address is
   unknown ("Not settled" 3, now settled by reading).
2. **The heartbeat formats `wasmkeep-node 1` and `2`** and their start
   guard. Section 6 lists them as removable; they were left out of this
   change, which keeps `cluster forget` and `cluster fence` saying "the
   format from before node leases" for such an object. To remove in
   `0.9.x` or `0.10`.
3. **The per-actor lease marker**, as section 6 says.

A relayed socket hand-off carries one sender, the call's, in `0.8.1` as
in `0.9`: the receiver takes the call and not the proof, and requiring
the sender changes nothing there. A test pins it.

### The mark of `cluster forget`

`cluster forget` deleted a released node lease after reading it twice;
a store's DELETE takes no condition, so a process that started under
that id between the second read and the delete lost its lease
([ADR 0020](0020-one-lease-per-node.md#long-released-node-leases-cluster-forget)).
The bucket adapter still has no conditional delete (`object_store`
0.14.2), so the removal is made of a conditional write and a delete.

1. **The mark.** `wasmkeep-node-forgotten 1\n<written-at ms>\n` at
   `nodes/<id>`: the header, then the operator's clock when it was
   written. Decoding is exact (the bytes the encoder writes, nothing
   else). It is `upgrade::FORMATS`' second row behind the floor: since
   `0.9`, no word.
2. **The command.** After its checks (released, older than 25 s, no
   ownership record names the node), `forget` replaces the lease with
   the mark, `If-Match` on the version it checked, then deletes the
   mark. A failed condition means the object changed: nothing is
   deleted, and the command says to run it again.
3. **The rule for a node.** Only `forget` writes over a mark or deletes
   it. A node's lease writes are all conditional, so none lands over a
   mark by accident; the start check adds the rest. A node that reads
   the mark under its id, at the check's first read or at any later
   one, judges nothing, waits until the key is absent (a read every
   half period), and then runs the whole check again, its ttl plus
   grace included: it creates its lease over no object. A first write
   whose condition fails because the mark landed after the check comes
   back to the same wait. So no lease a node wrote is deleted: if the
   node writes first, the command's condition fails; if the command
   writes first, the node's write is refused, or the node reads the
   mark and waits; if the command finishes first, the node starts as on
   a new id.
   The command's two requests follow each other at once, so a start
   check that was already watching the released lease usually reads the
   lease, then nothing, and never the mark. A released lease that is
   gone is taken the same way as the mark: nothing is judged and the
   whole check runs again, over no object. Its process stopped cleanly
   and nothing writes under the id, so its absence is no other writer.
   Only for a released lease: a live or fenced one, or a heartbeat from
   before node leases, that goes absent during the check is a deletion
   as before, and the start stops.
4. **The bound.** A mark still there four times a lease's ttl plus grace
   after the node first read it (60 s at the defaults) is one its
   command did not delete. The node does not start, reports `this
   node's lease is being removed by cluster forget`, and names the
   remedy: run `wasmkeep cluster forget <node>` again, which deletes a
   mark it finds (read right before the delete), whatever its age. The
   node does not take the mark over after the bound: a command that is
   only slow would then delete the node's lease, which is the gap this
   closes.
5. **Only after the finalize.** A node of `0.8.1` does not know the
   mark: starting under that id it would watch it as an object it
   cannot read and then write its lease over it. So the mark is written
   only when `gate` allows its row for the app the lease names: that
   app's floor at `0.9` or later (no node of `0.8.1` starts there any
   more), and no live node of the app older than `0.9`. The floor is
   read first, and the nodes are listed only when it allows the mark.
   Otherwise
   `forget` reads twice and deletes, as before `0.9`, and its answer
   says so and names what blocks the mark. A heartbeat from before node
   leases is removed that way too. The command takes no version of its
   own into account: the gate is about the nodes.
6. **Not a node.** Every reader of `nodes/<id>` skips the mark, and none
   counts it as an object this version cannot read:
   - the heartbeat listing and the reads by key (`membership`), so
     routing, the peers' pins, `cluster status`, `cluster upgrade
     status`, `finalize`'s two listings and every capability gate;
   - `upgrade::list_nodes`, so the skew check does not wait for it, a
     start is not refused for an object "rewritten meanwhile", and it
     does not make an app one that ran;
   - a record's reader (`frees`): as an absent object, it frees nothing,
     and no record names the node by then;
   - `cluster fence`: refused, saying to run `forget` again (a fence
     written over the mark would be deleted with it);
   - a peer's fence: not live, nothing to fence.
   `node_lease::forgotten` is the one test of "is this a mark", for
   commands that read `nodes/` themselves.
7. **`cluster status` names a mark that stayed.** A mark written more
   than 5 s ago was left by a command that did not finish. `cluster
   status` warns with one line, "forget did not finish; run `wasmkeep
   cluster forget <id>`": until then no node starts under that id.
   `cluster verify` says the same as a note (`note:`, and `notes` in
   `--json`): the mark is not a node and not a problem with what the
   bucket holds, so it changes neither the verdict nor the exit code. A
   younger mark is passed over in silence.

Limits, kept as documented:

- **A delete that lands late.** The delete of the mark is given up
  when it has had no answer for two thirds of a lease's ttl plus grace
  (10 s at the defaults), and the command then says to run it again;
  the next run reads the key first. So no delete is sent again long
  after one that was applied with its answer lost. A node creates its
  lease a full ttl plus grace after it first read the key absent, later
  than the last delete sent. What is left is a single request that the
  network holds for longer than that after it was sent.
- **Two `forget` of one node at once.** Each deletes the mark it read.
  If the first delete, a node's create and the second delete land in
  that order, the node's new lease is deleted: it stops at its next
  write and is started again. Availability only, and it needs two
  operators and a node start in the same second.
- **Before the finalize** the gap of ADR 0020 is as it was.
- **A whole-app restore in place** reads `nodes/` itself and takes an
  object it cannot decode as a node whose app is unknown. A mark that
  is there while it waits for the app's earlier nodes makes it take the
  longer of its waits, and one more round when the mark goes away, as
  any such object does. That reader was not changed here.
- **Nodes of `0.8.1` of another app on the same prefix** list the mark
  and skip it (their heartbeat decoder reads an unknown first line as
  nothing). One starting under the very id being forgotten, for
  another app that is not finalized, would write over the mark: the
  gate is per app, the app the released lease names, and a node id is a
  key of the whole prefix. A node id belongs to one app, so this needs
  a node id moved between apps during the removal.

Tests: the mark's decode (`a_forgotten_mark_is_exact_or_not_one`) and a
node's writes over it (`a_node_never_writes_over_a_forgotten_mark`),
the command's two steps (`node_lease::tests::forget`), every listing
(`membership::tests::a_forgotten_mark_is_not_a_node`,
`upgrade::tests::forgotten`), the gate before and after the finalize
(`the_forgotten_mark_waits_for_the_finalize`, and the command's own
`forget_marks_a_released_lease_only_after_the_finalize`), the start
check (`a_node_starting_while_its_lease_is_forgotten_waits_for_the_delete`,
`a_forgotten_mark_never_removed_stops_the_start`,
`a_forget_that_ends_between_two_reads_of_the_start_check_lets_the_start_go_on`),
the bounded delete
(`a_delete_of_the_mark_that_gets_no_answer_is_given_up`),
and one cluster case,
`tests/cluster/upgrade.rs`:
`a_node_forgotten_after_the_finalize_starts_again_with_its_lease`. No
simulation case: the lease protocol between nodes is unchanged, and the
node-lease simulation was run again over the changed module.

## Not settled

1. **The window after an upgrade** (Context, 1). Why a room's edges
   stay uneven after a node's restart, and whether placement, the
   upgrade steps or the limits change, is not decided here. The proof
   of section 8 does not pass until it is.
2. **The cost of `cluster status` and of the two new commands with many
   released node leases** (Context, 2): to measure, then bound or
   document.
3. *Settled by reading* (section 6): a serving `0.8.1` node of one app
   sends a forward with no proof while its cluster secret is not
   loaded, and when the client's address is unknown. The branch stays.
4. *Settled on 2026-11-25* (section 3, "A restore"): a restore into
   another prefix. A backup carries the app's floor, and a whole-app
   restore checks every manifest it writes through the gate and leaves
   the higher of the target's floor and the backup's, before anything
   else of the app.
5. **Rows of the gate table read once** (history content, alarm index,
   restore fence, control intents) and four items on the wire read only
   in comments: the take call, the socket hand-off relay, the placed
   mark sent to a node without `place`, and the apart refusal's reason.
   The mixed-version check exercises each; none was traced end to end
   for this record.
6. **What a `0.8.1` CLI does against a finalized cluster** beyond
   failing on a manifest it cannot parse. It has no floor check; phase 8
   is the test.
7. *Settled when built* (section 4, "`node below the floor`"): a
   `0.8.1` node that cannot read the floor after its first lease write
   stays live and checking, without serving. `finalize` sees it as a
   live older node and refuses; `cluster upgrade status` names it apart
   once the floor is above it.
8. *Settled when built* (section 3, "born finalized"): "no other node
   registered" is not proof of a fresh app, so the condition also needs
   no state and no tombstone under the app.
9. *Settled in review* (section 3): a fresh app deployed by a newer
   `wasmkeep` than its nodes. The first deploy creates the floor no
   higher than the lowest live version under the prefix, and nothing
   when that is `0.8`.
10. **No finalize without a live node.** After an upgrade done by
    stopping every node, `finalize` is refused until a node is live
    again. `0.9.x` must ship an offline path before `0.10`: a finalize
    accepted when every registered lease of the app is released or
    fenced and names the target version or a later one.
11. **An object under `nodes/` that does not decode** is never removed
    by a command: `cluster forget` reads only what it can decode (and
    its own mark, which is not such an object). It
    holds every start back by a lease's ttl plus grace and keeps its
    prefix's apps from being born finalized, until it is deleted from
    the bucket by hand.
12. **An unreadable floor record** has no repair command. Every node of
    the app refuses to start and every writer refuses to write until
    the object is rewritten by hand with the exact body of section 3 (or
    deleted, which reads as `0.8`). `app delete --force` passes it.
13. **`secrets rotate-key`** runs no writer's check (see "Built"); the
    other `secrets` writes do.
14. **A node of N-1 and a node of N+1 starting at the same moment**
    beside N can both pass the skew check, each listing before the
    other's first lease write. Each then sees the other as a live node
    outside its range only while running, which `cluster status` warns
    about and which stops neither. Low: an upgrade moves one node at a
    time.
