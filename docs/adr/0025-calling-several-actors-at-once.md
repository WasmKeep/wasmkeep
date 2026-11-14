# 0025. Calling several actors at once

- Status: accepted (2026-11-14), planned for 0.4.0
- Date: 2026-11-14
- Release: [0.4.0](../../ROADMAP.md#now)
- Follows: [ADR 0023](0023-the-front.md) ("Not decided here": concurrent
  calls).
- Amends: [ADR 0023](0023-the-front.md) ("one call at a time per front
  instance"; "Alternatives": a concurrent call).

## Context

`actors.call` blocks the guest until the callee answers, and a guest has
no way to start a second call before the first returns. A front that
gathers a page from N actors (a cart, a profile, a recommendation list)
pays the sum of N round trips, each a full event on the callee, and a
call to another node adds a signed hop to each. An actor that fans out
(a room asking its members, a job asking its shards) pays the same.

WASI 0.3 brings async functions to the component model, which would let
a guest start several calls and wait on them. It is not what the host or
the SDKs target in 0.4.0 (ADR 0001: WASI 0.2), and every language's
toolchain would have to follow.

What must not change: an actor runs one event at a time, in arrival
order (`actor.wit`, the package's first paragraph). A guest stays
single-threaded and blocks in one host call, as today.

## Decision

### The `calls` interface

```wit
/// Several calls between actors at once.
interface calls {
    use actors.{request, response, call-error};

    call-many: func(calls: list<tuple<string, request>>)
        -> list<result<response, call-error>>;
}

world actor-core {
    import calls;
    // ... as before
}

world front-core {
    import calls;
    // ... as before
}
```

- One host call takes a list of `(address, request)` pairs, the same
  `address` (`class/name`) and `request` as `actors.call`, starts every
  call at once, and returns when all have answered or the batch's
  deadline passed. The answers come **in input order**, one per entry:
  the list returned always has the length of the list given.
- The types are `actors`' own (`use actors.{...}`): no new type, no new
  `call-error` case (a case would break every built actor, ADR 0023).
- A new interface, not a function added to `actors`: a guest that does
  not use it does not import it, and its SDK need not wrap it.
- The package stays `@0.2.0`. Nothing is removed or changed; an
  interface is added to two worlds. A component built before imports
  `actors` without `calls` and links as before: the host linking more
  than a component imports is allowed. A component that imports `calls`
  is refused by an older host at deploy (an import the host does not
  provide), as any new import is.
- As for the front's world (ADR 0023), the project rule that a WIT
  change updates every SDK in the same change applies to changes of what
  exists; an added interface is adopted SDK by SDK. Rust
  (`calls::call_many`) and TypeScript (`actors.callMany`) have it in
  0.4.0; the other SDKs keep building unchanged (their toolchains either
  drop an unused import or import it, and the host provides it).

### What a batch does

- **Each entry is an `actors.call`**, made by the same host code: the
  same address check (`invalid-address`), the same 16 MiB request body
  limit (`too-large`), the same routing (local mailbox, owner hint,
  signed hop to another node, draining hand-off), the same caller value
  and the same chain. An entry's result is exactly what `actors.call`
  would have returned for it at that moment, but for the batch rules
  below.
- **At most 16 entries.** A longer batch starts none of its calls and
  returns `failed("too many calls: at most 16 in one call-many")` for
  every entry. The guest's own mistake, seen at once, with no partial
  effect. 16 is the parallelism a guest gets: a front page or a fan-out
  rarely needs more, and a bound keeps one event from opening an
  unbounded number of waits on the node (see "Limits"). An empty batch
  returns an empty list.
- **Bodies, each way, for the batch as a whole.** The requests of one
  batch together are at most 16 MiB (else every entry is `too-large`
  and none is started), and the responses together at most 16 MiB: an
  entry whose answer would take the batch past it is `too-large`, while
  the others keep theirs. One instance thus buffers the bodies one
  `actors.call` buffers today, not sixteen times that, and the answers
  fit in the guest's 128 MiB memory (headers are not counted, as they
  are not for one call).
  - Note (2026-11-14, review): an answer is counted as its bytes arrive,
    so which entry fails depends on arrival order: an answer still
    arriving holds what it has taken, and can make a sibling that would
    fit alone `too-large`. An answer whose `Content-Length` is larger
    than what is left fails before taking anything, unless it has no
    body whatever its length says (the answer to a HEAD, a 204, a 304):
    those are judged on what arrives. A partial answer that then fails
    gives its bytes back.
- **Order.** The calls run concurrently; no order between entries is
  promised, not even between two entries to the same callee. Each callee
  still runs its events one at a time, so two entries to one actor are
  two events there, one after the other.
- **Duplicates are allowed.** The same callee twice is two calls, as two
  `actors.call` in a row would be; refusing it would make the SDKs
  deduplicate requests that are not idempotent. Each counts on its own
  against every bound below.
- **Errors are per entry.** One entry's failure, timeout or refusal does
  not cancel the others. A trap in a callee is that entry's
  `failed(...)`.

### Deadline

One deadline for the batch:

- In an actor: the event's call deadline, as for `actors.call`: the
  event's wall (`wall_per_event`, 30 s by default) from when the event
  started, less the margin the event keeps to answer: 5 s, as the
  front's (ADR 0023), or a fifth of a wall shorter than 25 s. A batch
  entered after that deadline fails every entry with `timeout` at once
  (but one with an invalid address).
  - Note (2026-11-14, review): the first build gave an actor's batch,
    and its single calls, the whole wall from when the call started. The
    event's own wall, counted from the event's start, then always ran
    out first and cut the event: an actor never saw a `timeout` from a
    call, and a batch's answers were lost with the event.
- In a front: the front's call deadline (ADR 0023): 5 s before the
  request's wall ends, or the actors' call limit, whichever is first. A
  batch started with less than 5 s left fails every entry with `timeout`
  at once (but one with an invalid address, which is `invalid-address`,
  as for a single call).

Every entry starts at once and waits at most until that deadline. An
entry still waiting then is `timeout`; the entries that answered keep
their answers, and the caller, its event not cut, can use them. So a
batch takes as long as its slowest entry, never more than one
`actors.call` could wait.

### Chain and reentrancy

- Every entry carries the chain a single call from this caller would
  carry: the caller's own chain, plus the caller if it is an actor (the
  front is never in a chain, ADR 0023).
- An entry naming the caller itself, or any actor already waiting in the
  chain, is `reentrant`, at once, as `actors.call` is; the other entries
  run.
- Siblings are not in each other's chain: entries of one batch do not
  wait on each other, so a sibling's call to another sibling is
  legitimate (it waits for that sibling's current event, then runs).
- **A cycle between siblings ends in `timeout`.** If a batch calls B and
  C at once, B calls C and C calls B, each waits on an actor busy waiting
  on the other; neither chain lists the other, as with two independent
  chains today (`actors.call`'s doc: "a cycle across two independent
  chains ... ends in timeout"). A batch makes this reachable from one
  caller, where sequential calls could not. Every call involved gives up
  at its deadline (the outermost caller's first, as its event started
  first: it answers with `timeout` for those entries and the answers of
  the others), the events end, and the actors serve again; nothing
  stays stuck. The docs tell batch callees not to call each other.

### Limits

- **Front slots.** A batch runs in the front request's own slot: it takes
  no other slot, as a single call takes none.
- **Per actor (front).** Each entry counts against the front's per-actor
  bound (a quarter of the slots, ADR 0023) for as long as that entry
  waits, as a single call does: an entry past the bound fails at once
  with `failed("front busy: ...")`, the others run. Two entries to one
  actor count twice.
- **Per node (front).** The calls the node's fronts have waiting at once,
  entries and single calls alike, are at most the front's slots (100 by
  default): an entry past it fails at once with `failed("front busy: too
  many front calls waiting on this node")`, counted `busy`, and the
  others run. With one call per request this bound could never be
  reached; with batches it keeps the fronts from holding more of the
  node's actor instances than single calls could (ADR 0023 sized the
  slots as the part of the pool kept from actors), and from filling the
  resident pool with slow callees so that every other cold actor gets
  the capacity 503. A batch of 16 thus needs 16 free of the slots' worth
  of waits: on a node with few slots, fewer entries run at once.
  - Note (2026-11-14, review): added after review; the first build
    bounded entries per actor only.
- A callee's own limits are unchanged: its mailbox's queue (`callee
  overloaded`), the node's instance pool (the capacity 503, returned as
  a response), and its admission.
- **Actors fanning out.** An actor's batch has no bound but its 16
  entries, and a callee may batch in turn: a chain of batches can open
  16 events per level where sequential calls opened one. Each level is
  bounded by the node's instance pool and admission (a capacity refusal
  after the admission wait), and each call by its deadline, so nothing
  hangs; but one event can take the pool for that long. A bound per
  event or per chain is not decided here.
- **Metrics.** `wasmkeep_front_calls_total{outcome}` counts each entry of
  a front's batch, as a call; a batch refused as a whole (too many
  entries, requests too large) counts once, however long its list. Calls to other nodes are counted per entry
  by `wasmkeep_forwards_total{kind="call"}`.

### Between nodes

Nothing changes on the wire. Each entry for an actor owned elsewhere is
its own signed call (`cluster/signing.rs`), with its own
`x-wasmkeep-caller`, `x-wasmkeep-chain` and `x-wasmkeep-app`, exactly as
a single call; the receiving node cannot tell a batch's entry from a
call. Entries to different nodes run in parallel.

### SDKs

- **Rust**: `wasmkeep_sdk::calls::call_many(&[(address, request)])`,
  for actors and fronts (`wasmkeep_sdk::front::calls`), returning one
  `Result<Response, CallError>` per entry.
- **TypeScript**: `actors.callMany([[address, request], ...])` in
  `@wasmkeep/sdk` and `@wasmkeep/sdk/front`, returning one entry per
  call: a `CallResponse`, or a `CallError` (not thrown).
- **Later**: Python, Go, C, Zig, D and MoonBit; stated in
  `docs/languages.md`.

## Consequences

- A front gathering N actors pays about one round trip, not N.
- An actor fanning out holds its event open for the slowest callee, as
  it would for one call.
- A front request can hold up to 16 waits, within the node's bound of
  one wait per slot; one front request can thus take waits other front
  requests would have used.
- Sibling cycles, impossible with sequential calls, are possible and end
  in `timeout`.

## Alternatives

- **Async calls (start, then poll or wait).** A handle the guest keeps
  across host calls is a resource with a lifetime (dropped, leaked,
  outliving the event), more SDK work in every language, and what WASI
  0.3 will give natively. Not decided here.
- **A function in `actors`.** Every SDK generating the whole interface
  would then import it whether used or not; a separate interface is
  adopted SDK by SDK and costs a built component nothing.
- **Putting siblings in each other's chain**, so that a sibling cycle
  fails `reentrant` at once. It also refuses a sibling calling another
  sibling without a cycle, which is legitimate and works with sequential
  calls: a batch would mean something other than its calls in a row.
- **Refusing duplicate callees.** The callee serializes them anyway, and
  an SDK would have to refuse requests that are meant to repeat.
- **One `timeout` for the whole batch.** Discards the answers that came
  in time.
- **No total body limit.** 16 entries of 16 MiB each way would let one
  instance buffer 256 MiB on the host, past the guest's own memory.

## Not decided here

- Streaming calls, and async calls (WASI 0.3).
- A bound per target node on the calls a node's fronts have waiting
  (ADR 0023), and a bound on an actor's fan-out per event or per chain.
- A batch larger than 16, should measurements ask for it.
