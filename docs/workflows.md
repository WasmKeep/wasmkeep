# Workflows

Work that spans minutes to days and must survive restarts and failover:
reserve stock, wait for approval, charge, notify, retrying each part on
its own. `wasmkeep_sdk::workflow` runs it on ordinary actors, one actor per
workflow instance, with no platform feature behind it
([ADR 0019](adr/0019-workflows-as-an-sdk-pattern.md)). The Rust and
TypeScript SDKs have it ([TypeScript](#typescript) below); Go, Python, C
and Zig have the client side ([Go](#go), [Python](#python), [C](#c),
[Zig](#zig)).

## Writing a workflow

A workflow is a function. The runner calls it from the top every time the
instance moves forward; inside it, three calls record what happened:

```rust
use wasmkeep_sdk::workflow::{Run, Stop};

fn order(wf: &mut Run, input: &[u8]) -> Result<Vec<u8>, Stop> {
    let reservation: String = wf.step_keyed("reserve", |key| stock::reserve(input, key))?;
    wf.sleep("cool-off", 60_000)?;
    if wf.wait_for_event("approved", 86_400_000)?.is_none() {
        return Err(Stop::fail("not approved in time"));
    }
    let _: String = wf.step_keyed("charge", |key| payments::charge(&reservation, key))?;
    Ok(b"shipped".to_vec())
}
```

- **`step(name, f)`** calls `f` and, once it returns `Ok`, records its result
  (`String`, `Vec<u8>`, `u64`, `i64`, `bool` or `()`: convert other types
  to one of these, or implement `Stored`); every later run gets
  the recorded result without calling `f`. Until then `f` may run again:
  an error from `f` is retried with backoff (1 s doubling, 5 attempts by
  default), and an attempt cut before its result is recorded runs again
  (rule 2 below). When the attempts run out the workflow fails, or, with
  `try_step`, gets the error to handle.
- **`sleep(name, ms)`** and **`sleep_until(name, at)`** wait. The instance
  costs nothing meanwhile: its alarm brings it back.
- **`wait_for_event(name, timeout_ms)`** waits for an event delivered with
  `Client::send_event`, and returns its payload, or `None` after the
  timeout. Each wait takes the oldest event of that name not taken yet.
- The function returns the workflow's output, or `Err(Stop::fail(..))` to
  fail it. `?` passes the runner's own `Stop` (waiting) through.

### Two rules

1. **Between steps, the same decisions every time.** The function replays
   from the top, so it must reach the same steps in the same order given
   the same input and recorded results. Put anything that varies inside a
   step: the time, random numbers, reads of data, HTTP calls. Steps are
   identified by their position, so a loop may repeat a step name; a run
   that reaches a different step at a recorded position fails the
   workflow ("the code changed the order of its steps") rather than mixing
   two histories. Deploying new code onto running workflows is safe when
   it only adds steps after the recorded ones.
2. **A step may run more than once.** Its result is recorded after `f`
   returns, so a node failure or a cut in between runs it again. Make
   steps idempotent, or use `step_keyed`, which gives the step an
   idempotency key for the service it calls: unique to the step and to
   this run of the instance.

## The workflow class

The workflow is served as a class of its own, which uses its actors'
alarm, so it does nothing else:

```rust
use wasmkeep_sdk::workflow::{self, Config};

struct Orders;

impl wasmkeep_sdk::Actor for Orders {
    fn on_init() -> Result<(), String> {
        workflow::store::init().map_err(|e| e.to_string())
    }
    fn on_alarm() -> Result<(), String> {
        workflow::store::on_alarm(&Config::new(), order);
        Ok(())
    }
}

impl wasmkeep_sdk::http::Guest for Orders {
    fn handle(r: wasmkeep_sdk::http::IncomingRequest, o: wasmkeep_sdk::http::ResponseOutparam) {
        workflow::store::handle(&Config::new(), r, o)
    }
}

wasmkeep_sdk::export!(Orders);
```

`Config` sets the attempts, the backoff, how long a finished instance is
kept (`retention_ms`, 7 days by default) and how much of an event a run
spends on new steps before continuing in a new event (20 s of the 30 s).
Serve the class with `--access orders=internal`: only other actors start
and query workflows (the runner also refuses outside requests, `403`).

To try a client in another language without a Rust toolchain, a release
carries the example's workflow already built: `workflows-orders.wasm`
(the `orders` class of [`examples/workflows`](../examples/workflows)) and
`workflows-ledger.wasm` (the `ledger` class its steps call, so it needs
both), signed like the release's other files, with the notices of the
libraries compiled into them in `THIRD_PARTY_NOTICES-components.txt`.
[The example's README](../examples/workflows/README.md#run-it) builds them
from a clone instead.

```bash
# $v: the version of your wasmkeep binary, 0.2.0 or later
for f in workflows-orders.wasm workflows-ledger.wasm; do
  curl -fsSLO "https://github.com/WasmKeep/wasmkeep/releases/download/v$v/$f"
done
wasmkeep serve --actor orders=workflows-orders.wasm --actor ledger=workflows-ledger.wasm \
  --actor app=your-app.wasm --access orders=internal
```

## Starting and following instances

From any other actor:

```rust
use wasmkeep_sdk::workflow::Client;

let orders = Client::new("orders");
orders.start("order-42", b"item=book")?;     // idempotent while kept
let status = orders.status("order-42")?;     // state, wake time, output or error
orders.send_event("order-42", "approved", b"")?;
orders.cancel("order-42")?;
```

- `start` only records: the instance runs in its own event right after,
  never inside the caller's call. A step may therefore call back the actor
  that started it.
- `status` reports `Running`, `Waiting` (until when, for which events),
  `Completed` (with the output), `Failed` (with the error) or `Cancelled`,
  and how many positions are recorded.
- `send_event` and `cancel` to an id that was never started answer
  `Error::Unknown` and leave nothing behind; an event to a finished
  instance answers `Error::Finished`.
- `cancel` stops the instance at once; a step running at that moment
  finishes first.
- After its retention, a finished instance deletes itself. Its id is then
  unknown, and a `start` with it is a new run (new idempotency keys).

### Go

The [Go SDK](../sdks/go) (for TinyGo) has the **client** only: package
`workflow` starts and follows instances of a workflow class written with
the Rust runner, over the same protocol (the `wasmkeep-workflow/v1`
routes). As with its [kv and queue clients](kv-and-queues.md#go), it sends
through `wasmkeep.Caller`.

```go
import (
	"errors"

	"github.com/wasmkeep/wasmkeep/sdks/go/wasmkeep"
	"github.com/wasmkeep/wasmkeep/sdks/go/workflow"
)

orders := workflow.New("orders", wasmkeep.Caller)
status, err := orders.Start("order-42", []byte("item=book")) // *workflow.Status
status, err = orders.Status("order-42")                    // State, WakeAt, WaitingFor, Output, Error, Positions
err = orders.SendEvent("order-42", "approved", nil)
status, err = orders.Cancel("order-42")
if errors.Is(err, workflow.ErrUnknown) { /* never started, or past its retention */ }
if errors.Is(err, workflow.ErrFinished) { /* takes no more events */ }
```

`WakeAt` is 0 when there is none, `Output` nil when there is none (an
empty output is non-nil), `Error` empty when there is none. Errors are
`*workflow.Error` with the Rust SDK's kinds and messages (`ErrUnknown`,
`ErrFinished`, `ErrCall`, `ErrRefused`). [`examples/go-jobs`](../examples/go-jobs)
drives the `orders` class of [`examples/workflows`](../examples/workflows)
with the same order routes as its Rust app; that class is also
[in a release, already built](#the-workflow-class).

### Python

The Python SDK (`sdks/python`, module `wasmkeep.workflow`) has the
**client** only: it starts and follows instances of a workflow class
written with the Rust runner, over the same protocol, as
[its key-value and queue clients](kv-and-queues.md#python) do.
[`examples/py-jobs`](../examples/py-jobs) drives the `orders` class of
[`examples/workflows`](../examples/workflows) this way (the class is also
[in a release, already built](#the-workflow-class)).

```python
from wasmkeep import Actor, workflow

orders = workflow.Client("orders")


class App(Actor):
    def fetch(self, req):
        orders.start("order-42", "item=book")            # a Status; idempotent while kept
        orders.send_event("order-42", "approved", b"")
        try:
            status = orders.status("order-42")
        except workflow.WorkflowError as e:
            if e.kind == "unknown":                      # never started, or past its retention
                return "no such order\n"
            raise
        if status.state == "completed":
            return status.output                         # bytes
        return f"{status.state}, waiting for {status.waiting_for}\n"
```

The names and semantics are the Rust ones, with these differences:

- **`Status`** is a frozen dataclass: `state` is a string (`"running"`,
  `"waiting"`, `"completed"`, `"failed"` or `"cancelled"`, also in
  `workflow.STATES`), `wake_at` an `int` or `None`, `waiting_for` a list
  of names, `output` `bytes` or `None`, `error` a string or `None`,
  `positions` an `int`; `is_finished` is a property.
- **Errors are exceptions**: `WorkflowError` with `kind` `unknown` (404:
  no such instance), `finished` (409: an event to a finished instance),
  `call` (the call failed: the instance may or may not have recorded it)
  or `refused` (any other answer, with `status`), worded as Rust's
  `Display`.
- **Inputs and payloads** are `str` (sent as UTF-8) or `bytes`, and
  default to empty.

### C

The C SDK ([`sdks/c`](../sdks/c), declared in `wasmkeep_workflow.h`, which
`wasmkeep.h` includes; compile `wasmkeep_workflow.c` with the other SDK
files) has the client side only: the workflow class itself runs on the
Rust runner. It speaks the same protocol as `Client`, so it starts and
follows the same instances.

```c
static const wk_workflow orders = {.class_name = "orders"};  /* or wk_workflow_client("orders") */

wk_workflow_status s;
wk_workflow_error err;
if (!wk_workflow_start(&orders, wk_str("order-42"), wk_str("item=book"), &s, &err))
  return wk_reply(502, "%s\n", err.message);
wk_workflow_status_free(&s);
if (wk_workflow_get_status(&orders, wk_str("order-42"), &s, &err)) {
  /* s.state (WK_WORKFLOW_WAITING, ...), s.wake_at (0: none), s.waiting_for[0 .. s.waiting_count),
   * s.has_output and s.output, s.error (len 0: none), s.positions */
  wk_workflow_status_free(&s);
}
wk_workflow_send_event(&orders, wk_str("order-42"), wk_str("approved"), wk_str(""), &err);
wk_workflow_cancel(&orders, wk_str("order-42"), NULL, &err);  /* NULL: the status is not wanted */
```

- Each call returns `false` on failure and fills an optional
  `wk_workflow_error`: `WK_WORKFLOW_UNKNOWN`, `WK_WORKFLOW_FINISHED`,
  `WK_WORKFLOW_CALL` (the call failed; the instance may or may not have
  recorded it) or `WK_WORKFLOW_REFUSED` with `status` (200 for a malformed
  status), and the Rust message.
- `wk_workflow_get_status` is Rust's `status` (`wk_workflow_status` is the
  type). Ids, event names and payloads are `wk_bytes`; a status is freed
  with `wk_workflow_status_free`, and its bytes are NUL-terminated.

The jobs app in C ([`examples/c-jobs`](../examples/c-jobs)) has the
order routes of `examples/workflows/app`.

### Zig

The Zig SDK wraps the C client as `wk.workflow`
([`workflow.zig`](../sdks/zig/workflow.zig)), with Zig errors:

```zig
const orders = wk.workflow.Client.init("orders");   // a comptime constant is fine

fn example() !void {
    var status = try orders.start("order-42", "item=book");
    defer status.deinit();
    // status.state (.running, .waiting, ...), status.wake_at (?u64),
    // status.waitingCount() / waitingFor(i), status.output (?[]const u8),
    // status.failure (?[]const u8), status.positions
    try orders.sendEvent("order-42", "approved", "");
    var cancelled = try orders.cancel("order-42");
    cancelled.deinit();
}
```

`wk.workflow.Error` is `Unknown`, `Finished`, `Call` or `Refused`; the
message and status of the last failure are `wk.workflow.lastError()` and
`lastStatus()`. The jobs app in Zig
([`examples/zig-jobs`](../examples/zig-jobs)) has the same order
routes.

## What is guaranteed

- Every state reported (started, a step's result, a sleep's wake time, an
  event received, completed, failed, cancelled) is written with
  `exec-durable`: it survives any node failure.
- While an instance is not finished, its alarm is set, so a trap, a cut
  or a failover never leaves it stuck: the next attempt comes from the
  alarm, after the step's backoff. Runs that keep ending without the
  function returning (a trap outside any step, a replay over its CPU
  budget) back off too, and fail the instance after twice its attempts.
- A database error inside the runner is retried, never turned into a
  failed workflow.
- Every event makes progress: a run spends `run_budget_ms` on new steps
  and continues in a new event, but always runs at least one.
- One event at a time per instance: no two runs of the same workflow
  race.

## TypeScript

The same runner, tables and protocol in `@wasmkeep/sdk`, so a Rust client
follows a TypeScript workflow and the other way round. A waiting run
stops by throwing a `Stop` (let it through, or rethrow it from a
catch-all: `workflow.isStop(e)`), and step results are recorded as JSON:

```ts
import { env, http, workflow } from "@wasmkeep/sdk";

export default workflow.define(
  (wf, input) => {
    const reservation = wf.step("reserve", (key) => {
      const r = http.fetch(env.get("RESERVE_URL") ?? "", {
        method: "POST",
        headers: { "idempotency-key": key },
      });
      // Any status is a response: throw on failure, so it is retried
      // rather than recorded.
      if (r.status !== 200) throw new Error(`reserve answered ${r.status}`);
      return r.text();
    });
    wf.sleep("cool-off", 60_000);
    if (wf.waitForEvent("approved", 86_400_000) === undefined) {
      throw workflow.fail("not approved in time");
    }
    return `shipped: ${reservation}`;
  },
  // A function, read on each event: module code runs at build time, when
  // the class's configuration is not there yet.
  () => ({ maxAttempts: Number(env.get("ATTEMPTS") ?? 5) }),
);
```

- `wf.step(name, (key) => ...)` passes the step's idempotency key; an
  exception thrown from it is retried. `wf.tryStep` returns
  `{ ok }` or `{ error, attempts }` instead of failing the workflow.
- `wf.sleep`, `wf.sleepUntil` and `wf.waitForEvent` (the payload as bytes,
  or `undefined` after the timeout) work as in Rust.
- `new workflow.Client("orders")` has `start`, `status`, `sendEvent` and
  `cancel`; errors are `workflow.WorkflowError` (`kind`: `unknown`,
  `finished`, `call`, `refused`).
- Steps that call other services use `http.fetch`, the SDK's synchronous
  outbound HTTP, on either engine. A response whose body breaks before
  its end (the connection closes short of its `Content-Length` or inside a
  chunk, a reset, a timeout between bytes) throws a `FetchError` naming the
  incomplete body, so a step that checks only the status fails (retried
  while it has attempts left) rather than recording a cut answer. A body
  longer than `limit` is not an error: its first `limit` bytes are kept.
  The Rust SDK's `http::fetch` does the same, returning an error.

[`examples/ts-workflows`](../examples/ts-workflows) is the order of
`examples/workflows` in TypeScript, started and followed by the Rust app.

## Limits

- **A step's work fits in one event**: under 30 s of wall clock and the
  event's 50 ms of CPU, less what replay used. Longer work goes to
  something that reports back with `send_event`.
- **Replay costs about 1.5 µs a recorded step in every event** (a Rust
  workflow: 7.7 ms for a run over 5,000 steps,
  [measured](benchmarks.md#workflow-replay-against-recorded-steps)).
  Thousands of steps are fine; tens of thousands approach the event's CPU
  budget.
- **Large inputs cost CPU in TypeScript on QuickJS**, the default engine,
  which interprets: the input is read in every event, and a step that
  sends it as a request body copies it again. In the example, a 64 KiB
  input sent as a body fits in an event; at 256 KiB every run goes over
  the CPU budget before its first step, and the instance fails after
  twice its attempts (about a minute with the default backoff), saying
  so. StarlingMonkey handled 256 KiB. Keep large data out of the input
  (store it, and pass its key), or raise `--cpu-budget-ms`.
- **No list of instances**: an instance is found by its id. Keep a
  registry (a key-value prefix) if you need one.
- **Failover**: an instance is unavailable until a peer frees its dead
  node's actors (15 to about 21 s after that node's last write,
  [how](cluster.md#how-it-works)) and its actor is restored, and its alarm
  fires late, as for any actor: 24 s at the median and 36 s at worst with
  nodes next to their bucket, over twelve hours
  ([measured](benchmarks.md#three-nodes-against-a-real-bucket)).

The example in [`examples/workflows`](../examples/workflows) runs an order
through every case above.
