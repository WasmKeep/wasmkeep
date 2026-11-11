// Package wasmkeep writes WasmKeep actors in Go, built with TinyGo's
// wasip2 target. An actor is a few functions handed to [Serve] from an
// init function:
//
//	func init() {
//		wasmkeep.Serve(wasmkeep.Actor{
//			Init: func() error {
//				_, err := wasmkeep.Exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER NOT NULL)")
//				return err
//			},
//			Fetch: func(req *wasmkeep.Request) wasmkeep.Response {
//				wasmkeep.Exec("INSERT INTO hits (at) VALUES (?)", wasmkeep.NowMs())
//				n, _ := wasmkeep.QueryInt("SELECT count(*) FROM hits")
//				return wasmkeep.Text(200, strconv.FormatInt(n, 10)+" hits\n")
//			},
//		})
//	}
//
//	func main() {}
//
// The host delivers one event at a time to an actor, in arrival order, so
// handlers need no locks. The package wraps the bindings wit-bindgen-go
// generates from the actor world (committed in internal/gen); the
// component is encoded against the WIT in this module's wit directory.
package wasmkeep

// Actor is an actor's hooks. Every one is optional.
type Actor struct {
	// Init runs before the first event an instance handles with this
	// component version: once for a new actor, and again after each
	// deploy. Create or migrate tables here. An error fails the event; it
	// is retried with the next one.
	Init func() error
	// Fetch handles an HTTP request (and calls from other actors). Without
	// it every request is answered 404.
	Fetch func(req *Request) Response
	// Alarm runs when the pending alarm is due. An error (or a panic)
	// schedules a retry, so it must be idempotent.
	Alarm func() error
	// Open runs when a client upgrades a GET to a WebSocket on path (the
	// part after the actor's name, with its query). An error refuses it:
	// the connection is closed with code 1008 and the error as reason.
	// Without Open, connections are accepted when Message is set and
	// refused otherwise.
	Open func(conn uint64, path string) error
	// Message runs when a message arrives on one of the actor's
	// connections: text (binary false) or binary.
	Message func(conn uint64, data []byte, binary bool)
	// Close runs when a connection closed (by the client, by [Close], or
	// by the host).
	Close func(conn uint64, code uint16, reason string)
}

// The actor Serve registered.
var current Actor

// Serve makes a the actor this component runs. Call it from an init
// function (TinyGo runs init functions before the host's first call;
// main is not called), once.
func Serve(a Actor) {
	current = a
}
