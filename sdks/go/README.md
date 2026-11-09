# WasmKeep for Go: the kv, queue and workflow clients

The client side of WasmKeep's [key-value and queues](../../docs/kv-and-queues.md#go)
([ADR 0015](../../docs/adr/0015-kv-and-queues-as-sdk-patterns.md)) for Go
actors built with TinyGo. Go is experimental and has no full SDK yet: this
module holds only the clients, which speak `/wasmkeep-kv/v1` and
`/wasmkeep-queue/v1` byte for byte like the Rust and TypeScript SDKs, so
callers in every language share the same shards and queues. The shard and
queue classes stay the Rust components from
[`examples/jobs`](../../examples/jobs). The `workflow` client likewise
starts and follows [durable workflows](../../docs/workflows.md#go)
([ADR 0019](../../docs/adr/0019-workflows-as-an-sdk-pattern.md)) written
with the Rust runner, over `/wasmkeep-workflow/v1`.

| Package | |
| --- | --- |
| `call` | `call.Caller`, the function the clients send requests through, and `call.Failure`, which words a failed call as the other SDKs do |
| `kv` | `kv.New(class, shards, caller)`: `Get`, `GetFresh`, `Put`, `PutWith`, `CompareAndSet`, `Delete`, `DeleteIf`, `List`, `ListAfter`, `WithReadCache`; `ShardFor`, `ShardName` |
| `queue` | `queue.New(address, caller)`: `Send`, `SendDelayed`, `SendBatch`, `Lease`, `Ack`, `Retry`, `RetryAfter`, `Settle`, `Stats`, `DeadLetters`; `IsDelivery`, `CallerFrom`, `HandleDelivery` for push consumers |
| `workflow` | `workflow.New(class, caller)`: `Start`, `Status`, `SendEvent`, `Cancel`; `workflow.Status`, `workflow.State` |

## Using it

The module imports no generated bindings, so it works whatever module path
an app's `wit-bindgen-go` bindings have. The app hands its own
`actors.Call` binding to the clients:

```go
func callActor(address, method, path string, headers [][2]string, body []byte) (uint16, []byte, error) {
	r := actors.Call(address, actors.Request{
		Method:  method,
		Path:    path,
		Headers: cm.ToList(headers),
		Body:    cm.ToList(body),
	})
	if e := r.Err(); e != nil {
		detail := ""
		if s := e.InvalidAddress(); s != nil {
			detail = *s
		} else if s := e.Failed(); s != nil {
			detail = *s
		}
		return 0, nil, call.Failure(address, e.String(), detail)
	}
	return r.OK().Status, r.OK().Body.Slice(), nil
}

var store = kv.New("kv", 8, callActor).WithReadCache(60_000)
var jobs = queue.New("queue/jobs", callActor)
var orders = workflow.New("orders", callActor)
```

Until the module is published, reference it from the app's `go.mod` with a
`replace` directive, as [`examples/go-jobs`](../../examples/go-jobs) does:

```text
require github.com/wasmkeep/wasmkeep/sdks/go v0.0.0
replace github.com/wasmkeep/wasmkeep/sdks/go => ../../sdks/go
```

The [guide](../../docs/kv-and-queues.md#go) lists how the Go API differs
from Rust's (errors as values, zero values as defaults, string keys).

## Tests

```bash
go test ./...   # golden bytes and shard choices computed from Rust, the read cache, the clients against a fake caller
go vet ./...
```

`crates/wasmkeep-server/tests/jobs_langs.rs` (`go`) runs the jobs app built
with this module against the Rust classes, and
`crates/wasmkeep-server/tests/workflows_langs.rs` (`go`) drives an order of
the Rust `orders` workflow class through it.

The module keeps to Go 1.24 and to packages TinyGo handles well
(`encoding/binary`, `strconv`, `strings`, `sort`, `time`; no `fmt` or
reflection), and has no dependencies.
