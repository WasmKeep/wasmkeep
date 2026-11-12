# WasmKeep actors in Go

Write [WasmKeep](../../README.md) actors in Go, built with TinyGo's
`wasip2` target. The `wasmkeep` package wraps the whole host API (storage,
alarms, calls between actors, WebSockets, configuration, outbound HTTP);
the `kv`, `queue` and `workflow` packages are the clients of WasmKeep's
[key-value and queues](../../docs/kv-and-queues.md#go) and
[durable workflows](../../docs/workflows.md#go).

```go
package main

import (
	"strconv"

	"github.com/wasmkeep/wasmkeep/sdks/go/wasmkeep"
)

func init() {
	wasmkeep.Serve(wasmkeep.Actor{
		// Once per deployed version, before any other event: create tables here.
		Init: func() error {
			_, err := wasmkeep.Exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)")
			return err
		},
		// One request at a time, in arrival order: no locks needed.
		Fetch: func(req *wasmkeep.Request) wasmkeep.Response {
			if _, err := wasmkeep.Exec("INSERT INTO hits (at) VALUES (?)", wasmkeep.NowMs()); err != nil {
				return wasmkeep.Text(500, err.Error()+"\n")
			}
			n, _ := wasmkeep.QueryInt("SELECT count(*) FROM hits")
			return wasmkeep.Text(200, strconv.FormatInt(n, 10)+" hits on "+req.Path+"\n")
		},
		// WebSockets: set only the hooks you need.
		Message: func(conn uint64, data []byte, binary bool) {
			if !binary {
				wasmkeep.Broadcast(string(data))
			}
		},
	})
}

func main() {}
```

Save it as `main.go` in a new directory, add the module, build it with
TinyGo against the WIT the module ships (`wit/`), and serve it (see also
[`examples/go-counter/build.sh`](../../examples/go-counter/build.sh)):

```bash
go mod init hits
go get github.com/wasmkeep/wasmkeep/sdks/go@v0.2.0
go mod tidy   # records the module's one dependency
SDK=$(go list -m -f '{{.Dir}}' github.com/wasmkeep/wasmkeep/sdks/go)
tinygo build -target=wasip2 -no-debug -opt=2 \
  --wit-package "$SDK/wit" --wit-world actor-tinygo -o actor.wasm .
wasmkeep serve --actor hits=actor.wasm
```

TinyGo 0.39 (with Go 1.24) is what CI builds with; 0.43 (with Go 1.27) was
tried by hand: the three examples build with it.
Nothing else is needed: the bindings `wit-bindgen-go` generates are
committed in `internal/gen`. The `actor-tinygo` world is the actor world
plus the `wasi:filesystem` imports TinyGo's runtime makes whether or not a
program uses them (the host provides them inert: no directories).

## Install

Add the module to an app at the version of the WasmKeep release it runs
on, as above (`go get github.com/wasmkeep/wasmkeep/sdks/go@v0.2.0`, then
`go mod tidy` once the app imports a package).

The module carries the same version number as the server (in the
repository, the module's own tag is `sdks/go/v0.2.0`). The `wasmkeep`
package starts at `v0.2.0`: `v0.1.0` has only the `kv`, `queue` and
`workflow` clients.

The examples in this repository reference the checkout with a `replace`
directive instead:

```text
require github.com/wasmkeep/wasmkeep/sdks/go v0.0.0
replace github.com/wasmkeep/wasmkeep/sdks/go => ../../sdks/go
```

Outside the repository (an example copied elsewhere, for instance), delete
both lines and run the two commands above. `go get` writes the `require`
line at the release's version; it fails while the line still names
`v0.0.0`, which is not a published version.

## API

| | |
| --- | --- |
| Hooks | `wasmkeep.Serve(wasmkeep.Actor{...})` from an `init` function: `Init`, `Fetch`, `Alarm`, `Open` (return an error to refuse), `Message`, `Close`; each is optional. Without `Open`, connections are accepted when `Message` is set. An error from `Init` or `Alarm` fails the event (an alarm is retried) |
| `Exec(sql, params...)`, `ExecDurable(sql, params...)` | Rows changed, or a `*SQLError` (`Kind`, `Message`). Parameters are `nil`, `bool`, integers, floats, `string`, `[]byte` |
| `Query(sql, params...)` | `*Rows`: `Columns`, `Values` (`nil`, `int64`, `float64`, `string`, `[]byte`), `Len()`, `Get`, `Int`, `Float`, `String`, `Bytes`, `IsNull`, `Column(name)` |
| `QueryInt(sql, params...)` | First column of the first row |
| `SetAlarm(ms)`, `GetAlarm()`, `ClearAlarm()` | The actor's single alarm, ms since the Unix epoch |
| `Call(address, CallRequest{...})` | Calls another actor (`class/name`); a `Response`, or a `*call.Error` when there was no answer |
| `Caller` | The `call.Caller` the `kv`, `queue` and `workflow` clients take |
| `Send`, `SendBinary`, `Broadcast`, `BroadcastBinary`, `Close`, `Connections` | WebSockets; `Send` returns a `*SendError` (`unknown-connection`, `backpressure`) |
| `Request` | `Method`, `Path` (with the query), `Headers`; `Header(name)`, `LookupHeader`, `Body()`, `Text()`, `ReadBody()` |
| `Text(status, body)`, `Bytes(status, type, body)`, `JSON(status, json)`, `Response{...}` | Responses; `.WithHeader(name, value)` adds one |
| `Fetch(url, FetchOptions{...})` | Outbound HTTP, synchronous: `Method`, `Headers`, `Body`, `Timeout`, `Limit`; a `*Fetched` (`Status`, `Headers`, `Body`) |
| `Env(name)` | The class's configuration value or secret (`deploy --config`, `secrets set`, `serve --env`; last wins), and whether it is set |
| `Self()` | This actor's `Address` (`Tenant`, `Class`, `Name`) |
| `DeleteSelf()`, `NowMs()` | |

A panic in a hook traps the instance: the event fails, as an error does.
The full documentation is in the package comments (`go doc ./wasmkeep`).

## Key-value, queues and workflows

| Package | |
| --- | --- |
| `call` | `call.Caller`, the function the clients send requests through, and `call.Failure`, which words a failed call as the other SDKs do |
| `kv` | `kv.New(class, shards, caller)`: `Get`, `GetFresh`, `Put`, `PutWith`, `CompareAndSet`, `Delete`, `DeleteIf`, `List`, `ListAfter`, `WithReadCache`; `ShardFor`, `ShardName` |
| `queue` | `queue.New(address, caller)`: `Send`, `SendDelayed`, `SendBatch`, `Lease`, `Ack`, `Retry`, `RetryAfter`, `Settle`, `Stats`, `DeadLetters`; `IsDelivery`, `CallerFrom`, `HandleDelivery` for push consumers |
| `workflow` | `workflow.New(class, caller)`: `Start`, `Status`, `SendEvent`, `Cancel`; `workflow.Status`, `workflow.State` |

They speak `/wasmkeep-kv/v1`, `/wasmkeep-queue/v1` and
`/wasmkeep-workflow/v1` byte for byte like the Rust and TypeScript SDKs, so
callers in every language share the same shards and queues; the shard,
queue and workflow classes stay the Rust components from
[`examples/jobs`](../../examples/jobs) and
[`examples/workflows`](../../examples/workflows).

```go
var store = kv.New("kv", 8, wasmkeep.Caller).WithReadCache(60_000)
var jobs = queue.New("queue/jobs", wasmkeep.Caller)
var orders = workflow.New("orders", wasmkeep.Caller)
```

They import no bindings themselves: any `call.Caller` works, which is how
their tests run on the host against a fake one.

## Examples and tests

- [`examples/go-counter`](../../examples/go-counter): the counter.
- [`examples/go-chat-room`](../../examples/go-chat-room): the chat room.
- [`examples/go-jobs`](../../examples/go-jobs): the jobs app, over the
  key-value, queue and workflow clients.

They pass the same black-box tests as the Rust examples
(`crates/wasmkeep-server/tests/languages.rs`, `chat.rs`, `jobs_langs.rs`,
`workflows_langs.rs`), and `fetch_langs.rs` checks `Fetch` against the
Rust SDK's `http::fetch`.

```bash
go test ./...   # value conversion, rows, requests, URLs; the clients' golden bytes, shard choices and read cache
go vet ./...
./scripts/generate.sh   # regenerate internal/gen and wit after a change to crates/wasmkeep-wit (CI checks they match)
```

`scripts/generate.sh` needs `wit-bindgen-go` v0.7.0
(`go install go.bytecodealliance.org/cmd/wit-bindgen-go@v0.7.0`).

The module keeps to Go 1.24 and to packages TinyGo handles well (no `fmt`
or reflection). Its one dependency is `go.bytecodealliance.org/cm`, the
runtime package of the generated bindings.
