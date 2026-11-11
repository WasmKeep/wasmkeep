# 0019. Workflows as an SDK pattern built on actors

- Status: accepted (2026-11-06). Built: the Rust runner and client
  (`crates/wasmkeep-sdk/src/workflow.rs`, `examples/workflows`,
  `crates/wasmkeep-server/tests/workflows.rs`), replay measured, and the
  TypeScript runner and client with `http.fetch`
  (`sdks/typescript/src/workflow.ts`, `examples/ts-workflows`,
  `crates/wasmkeep-server/tests/workflows_ts.rs`), and clients in the Go,
  Python, C and Zig SDKs (each driving the Rust runner from its jobs
  example, `crates/wasmkeep-server/tests/workflows_langs.rs`). Every step
  listed at the end is done. Revised the same day after an adversarial
  review of the first draft; the decisions on its open questions are at
  the end.
- Date: 2026-11-06
- Milestone: [M4](../../ROADMAP.md#m4-operating-a-cluster), alongside ADR 0015

## Context

Some work spans minutes to days and must survive restarts and failover:
sign a user up, then send a welcome mail, wait three days, send a
follow-up; charge a card, then provision, then notify, retrying each part
on its own; wait for a person's approval before going on. Cloudflare calls
these Workflows, Temporal and others "durable execution". Today an
application builds it by hand: a table of states, an alarm, a switch over
the state in `on-alarm`.

ADR 0015 set the approach for such needs: a stable API in the SDKs, backed
by ordinary actors, with no host function and no WIT change, so a platform
primitive can replace the backend later. Everything a workflow needs exists
for one actor:

- its SQLite database, with `exec-durable` for writes that must survive
  any node failure (ADR 0007);
- one alarm, kept in the actor's database, that survives traps and
  failover and is delivered at least once (ADR 0009);
- one event at a time, so no two runs of the same workflow race;
- outbound HTTP and calls to other actors for the work itself;
- `delete-self` to remove a finished workflow.

The limits that shape the design: an event may use 50 ms of CPU by
default (`--cpu-budget-ms`) and 30 s of wall-clock time, including the
time it waits on calls; an actor has a single alarm; an actor cannot call
an actor already waiting in its call chain (re-entry is refused); and
handlers are synchronous in every SDK (TypeScript included).

## Decision

### One actor per workflow instance

A workflow is a class the application serves with the SDK's runner, for
example `signup`. Each instance is one actor of that class, named by its
id: `signup/user-42`. The class uses its actor's alarm for everything it
schedules, so it does nothing else; the application's other actors start
and query workflows through a client, as with queues.

### Runs happen on the alarm, never in the caller's call

`start` and `send_event` only **record** (durably) and **set the alarm to
now**, then answer. The workflow runs in its own `on-alarm` event:

- the caller's call is short, so it never holds the caller near its own
  30 s;
- the run's steps are not in the caller's call chain, so a step may call
  back the actor that started it (inside the call, that call would be
  refused as re-entry and burn an attempt);
- `status` and `cancel` wait behind at most one run's event, not behind a
  caller's.

### Every move forward is durable, and an alarm is always pending

The runner keeps one rule: **while an instance is not finished, its alarm
is set**, and every state it reports was written with `exec-durable`.

- Before running anything new in an event, the runner sets the alarm to a
  **watchdog** time (now plus the step's backoff). The alarm lives in the
  actor's database (ADR 0009), so a trap, a CPU or wall-clock cut, or a
  failover leaves it set, and the next attempt comes from it. Without it,
  a step that trapped in an event nobody retries would leave the instance
  waiting forever.
- `start`'s input, every recorded step result, a sleep's wake time, a
  waited-for event, and every change of status (to waiting, completed,
  failed or cancelled) are `exec-durable`. An answer the runner gives
  (`started`, `waiting until ...`) is never undone by a node failing.
- `on-alarm` returns success and keeps its own backoff: the host's alarm
  retry is not part of the design.

### Steps, replayed

The application writes the workflow as one function, run from the top on
every event that moves it forward:

```rust
fn signup(wf: &mut Run, input: &Signup) -> Result<Value, Stop> {
    let user = wf.step("create the account", || accounts::create(input))?;
    wf.step("send the welcome mail", || mail::welcome(&user))?;
    wf.sleep("before the follow-up", Duration::from_secs(3 * 86_400))?;
    if wf.wait_for_event("confirmed", Duration::from_secs(7 * 86_400))?.is_none() {
        wf.step("send a reminder", || mail::reminder(&user))?;
    }
    Ok(user.id.into())
}
```

- **Positions, not names, identify a step.** The runner counts the calls
  to `step`, `sleep` and `wait_for_event` in a run: the `n`th call is
  position `n`. A recorded position returns its recorded result; the name
  given there must equal the recorded one, or the workflow fails with "the
  code changed the order of its steps" instead of mixing two histories.
  So a loop may run the same step name many times, each at its own
  position.
- **`step(name, f)`** at a new position: if a backoff is pending for it
  (`next_attempt_at` later than now), the run stops until then (an event
  delivered meanwhile does not call `f` early). Otherwise the runner counts
  an attempt and records the error so far (a plain write: a trap inside
  `f` then counts, and only a node failure in between loses the count),
  runs `f`, and records the result with `exec-durable`. A failure (an
  error returned; a trap or cut, seen as an attempt without a result) sets
  `next_attempt_at` by backoff (1 s doubling to 5 min, 5 attempts by
  default, `StepOptions`) and stops. When the attempts run out, `step`
  returns the last error to the workflow, which may handle it or fail.
- **`sleep(name, duration)`** and **`sleep_until(name, at)`**: the first
  time, record the wake time durably (so a lost write cannot restart the
  sleep from a later "now") and stop until then.
- **`wait_for_event(name, timeout)`**: takes the oldest unconsumed event
  of that name delivered to the instance and returns its payload, or
  `None` after the timeout; until then the run stops. Events are a queue
  per name, each consumed by one wait, so a loop can wait for "approved"
  more than once. An event no wait consumes stays until the workflow
  finishes.
- **One event runs as far as it can**: recorded positions are lookups,
  loaded in one query; new steps run one after another until the workflow
  waits, finishes, or the event has used a set share of its wall-clock
  time, when the alarm (already set to now) continues it in a new event.

The tables:

```sql
CREATE TABLE wasmkeep_wf (id INTEGER PRIMARY KEY CHECK (id = 1), status TEXT NOT NULL,
                         run_id TEXT NOT NULL, input BLOB NOT NULL, output BLOB,
                         error TEXT, cancel_requested INTEGER NOT NULL DEFAULT 0,
                         created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL);
CREATE TABLE wasmkeep_wf_step (pos INTEGER PRIMARY KEY, name TEXT NOT NULL, kind TEXT NOT NULL,
                              result BLOB, error TEXT, attempts INTEGER NOT NULL DEFAULT 0,
                              next_attempt_at INTEGER, wake_at INTEGER);
CREATE TABLE wasmkeep_wf_event (seq INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL,
                               payload BLOB NOT NULL, received_at INTEGER NOT NULL,
                               consumed_at_pos INTEGER);
```

### What replay asks of the code

Between steps, the workflow function must be **deterministic**: given the
same input and the same recorded results, it must reach the same steps in
the same order. Anything that varies (the time, random numbers, a read of
this or another actor's data, an HTTP call) goes inside a step, whose
result is then fixed. The position check catches the common breakage (a
deploy that reorders or removes steps of running instances). A deploy that
changes a step's result type is not caught by it: decoding the recorded
result fails, and the workflow fails with that error. Changes that only
add steps after the recorded ones are safe; anything else needs a new
class (or a version in its name) for new instances while old ones finish.

**A step may run more than once.** Its result is recorded after `f`
returns; if the node dies, the event is cut or the recording fails in
between, the next attempt calls `f` again. Steps must be idempotent, or
carry an idempotency key to the service they call: `Run::step_key()` is
the instance's `run_id` (random, made at `start`) and the position, so a
later run under the same id (after retention, below) never reuses a key.
This is the same promise as every durable-execution system that records
results after the call, and the docs say so first.

### The client

`workflow::Client::new("signup")`, every call an `actors.call` that only
reads or records:

- `start(id, input)`: records the input, a new `run_id` and `running`, sets
  the alarm to now. If the instance exists, it changes nothing and returns
  its status, so a start is idempotent while the instance is kept.
- `status(id)`: `running`, `waiting` (until when, or for which event),
  `completed` (with the output), `failed` (with the error), `cancelled`, or
  unknown.
- `send_event(id, name, payload)`: records the event and sets the alarm
  to now. To an unknown id: refused (`404`), not buffered.
- `cancel(id)`: marks the instance `cancelled` at once and replaces its
  alarm with the retention alarm. A step running at that moment is in an
  event the cancel waits behind, so it finishes first and its result is
  kept, unused.

**Unknown ids leave nothing behind.** Any call creates the actor (and its
empty tables, from `on-init`). An instance with no `wasmkeep_wf` row answers
`404` to `status`, `send_event` and `cancel`, and deletes itself
(`delete-self`) in the same event.

As with ADR 0015, an instance answers only calls carrying
`x-wasmkeep-caller`, and the docs recommend serving the class with
`--access CLASS=internal`. The protocol between client and runner lives
under `wasmkeep-workflow/v1` and is internal to the SDK.

### Retention

A finished workflow keeps its status and output for a retention period
(7 days by default, `Config::retention`), then deletes itself from its
alarm. After that the id is unknown, and **a `start` with that id is a new
run**: new `run_id`, its steps run again. An application that must not run
twice keeps the id beyond retention itself, or sets a longer one.

### TypeScript needs outbound HTTP first

The TypeScript SDK had no outbound HTTP when this was decided: QuickJS,
the default engine (ADR 0018), has no `fetch`, and StarlingMonkey's
`fetch` is asynchronous, which a synchronous handler cannot await. A TypeScript step could only call
other actors. So the TypeScript runner comes after a synchronous HTTP
helper in its SDK (over `wasi:http/outgoing-handler`, as the Rust SDK's
`http::fetch`), which is useful on its own. That helper is now
`http.fetch` (`sdks/typescript/src/http.ts`), and the runner followed it.

### Limits, stated plainly

- **Each event has the actor's budgets**: 50 ms of CPU and 30 s of wall
  clock. Replay costs a lookup and a decode per recorded position, in
  every event: O(n) per event, O(n²) over a workflow's life. Measured for
  a Rust guest: about 1.5 µs a recorded step, 7.7 ms for a run over 5,000
  ([benchmarks](../benchmarks.md#workflow-replay-against-recorded-steps));
  TypeScript is not measured yet, and decoding is about 1.6× slower on
  QuickJS. A workflow expected to run tens of thousands of steps should
  end and start a new instance with its state ("continue as new", left
  for later).
- **One step's work fits in one event**: under 30 s of wall clock, less
  what replay took. Longer work is handed to something that reports back
  with `send_event`.
- **At-least-once steps**, as above.
- **No list of instances**: an instance is an actor found by its id. A
  list needs a registry the application keeps (a KV prefix, for example).
- **Failover**: an instance is unavailable from its node's last write
  until 15 to about 21 s later (ADR 0020), then the restore, and its
  alarm fires up to about 25 s late, as for any actor
  ([concepts](../concepts.md)).
- **Results live in the instance's database**: large outputs belong in the
  application's storage, with a reference in the step.

### Not done here

- Child workflows, parallel steps, "continue as new", timers finer than an
  alarm's precision. They fit the same tables later.
- A dashboard or a list of instances.
- Runners in Python, Go, C and Zig (see Decisions below). A client in every
  SDK is cheap: it is calls, like the queue client.

## Consequences

- Workflows ship as a library: no WIT, host or wire change, and they
  inherit replication, failover and access rules from actors.
- The application owns the determinism rule; the runner catches reordered
  steps and states what it cannot catch.
- A platform primitive later could keep the same client and the same
  `step`, `sleep` and `wait_for_event`.

## Decisions (2026-11-06)

Decisions on the draft's open questions:

1. **Languages**: the Rust runner first, then TypeScript once its SDK has
   synchronous outbound HTTP; clients in every SDK. Runners in the other
   languages when someone asks.
2. **Retention**: finished workflows are kept 7 days by default
   (configurable), then delete themselves; a later `start` with the same
   id is a new run.
3. **Long steps**: a step's work fits in one event (30 s); longer work is
   handed off and reports back with `send_event`. Steps spanning events
   are not planned.

Delivered in this order: the Rust runner and client with the tables above
and an example; a replay-cost measurement; TypeScript's synchronous
outbound HTTP; the TypeScript runner; clients in the other SDKs.
