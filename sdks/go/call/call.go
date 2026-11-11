// Package call is how the kv, queue and workflow clients reach other
// actors: through a function the app supplies. In an actor that is
// wasmkeep.Caller, the SDK's own call; the clients import no bindings
// themselves, so their tests run with plain `go test` on the host
// against a fake caller.
//
//	store := kv.New("kv", 8, wasmkeep.Caller)
package call

// Caller sends one request to the actor at address (`class/name`) and
// returns the callee's status and body. It returns an error only when the
// call itself failed (an answer, whatever its status, is not an error):
// build that error with [Failure], so its message reads as in the Rust and
// TypeScript SDKs.
type Caller func(address, method, path string, headers [][2]string, body []byte) (uint16, []byte, error)

// Error is a failed call. Its message is the one the Rust and TypeScript
// SDKs give for the same failure.
type Error struct {
	// Address is the callee (`class/name`).
	Address string
	// Kind is the host's call-error case: "invalid-address", "reentrant",
	// "too-large", "timeout" or "failed".
	Kind string
	// Detail is the case's text, for "invalid-address" and "failed".
	Detail string
}

func (e *Error) Error() string {
	switch e.Kind {
	case "invalid-address":
		return "invalid address " + e.Address + ": " + e.Detail
	case "reentrant":
		return "call to " + e.Address + " would re-enter an actor already in this call chain"
	case "too-large":
		return "call to " + e.Address + ": body too large"
	case "timeout":
		return "call to " + e.Address + " timed out"
	case "failed":
		return "call to " + e.Address + " failed: " + e.Detail
	}
	// A case this SDK does not know (a newer host): name it.
	if e.Detail == "" {
		return "call to " + e.Address + " failed: " + e.Kind
	}
	return "call to " + e.Address + " failed: " + e.Kind + ": " + e.Detail
}

// Failure is the error for a failed call to address: kind is the host's
// call-error case name (what the generated variant's String returns),
// detail its text when it has one.
func Failure(address, kind, detail string) error {
	return &Error{Address: address, Kind: kind, Detail: detail}
}
