package workflow

import (
	"strings"
	"unicode/utf8"

	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

// State is where an instance stands.
type State string

const (
	// Running: it has work to do now.
	Running State = "running"
	// Waiting: for a sleep, a retry's backoff or events.
	Waiting State = "waiting"
	// Completed: the workflow returned; Status.Output holds its output.
	Completed State = "completed"
	// Failed: a step ran out of attempts, or the workflow failed;
	// Status.Error says why.
	Failed State = "failed"
	// Cancelled: Cancel stopped it.
	Cancelled State = "cancelled"
)

// IsFinished reports whether the instance will run no further step.
func (s State) IsFinished() bool {
	return s == Completed || s == Failed || s == Cancelled
}

func parseState(s string) (State, bool) {
	switch st := State(s); st {
	case Running, Waiting, Completed, Failed, Cancelled:
		return st, true
	}
	return "", false
}

// Status is an instance's status, as [Client.Status] reports it.
type Status struct {
	// State is where it stands.
	State State
	// WakeAt is, when waiting, until when at the latest (ms since the
	// Unix epoch); 0 if none.
	WakeAt uint64
	// WaitingFor is, when waiting for events, their names.
	WaitingFor []string
	// Output is, when completed, the workflow's output; nil otherwise (an
	// empty output is non-nil).
	Output []byte
	// Error is, when failed, why; empty otherwise.
	Error string
	// Positions is how many positions are recorded so far (steps, sleeps
	// and waits).
	Positions uint64
}

// encodeStatus is the Rust SDK's encode_status, byte for byte: the state,
// the wake time (0: none), the event names joined with "\n", whether there
// is an output (1/0), the output, the error (empty: none), the positions.
func encodeStatus(s *Status) []byte {
	var w wire.Writer
	hasOutput := uint64(0)
	if s.Output != nil {
		hasOutput = 1
	}
	return w.Bytes([]byte(s.State)).
		U64(s.WakeAt).
		Bytes([]byte(strings.Join(s.WaitingFor, "\n"))).
		U64(hasOutput).
		Bytes(s.Output).
		Bytes([]byte(s.Error)).
		U64(s.Positions).
		Finish()
}

// decodeStatus reads what encodeStatus wrote. As in Rust, an unknown
// state or text that is not UTF-8 is malformed; bytes after the positions
// are ignored.
func decodeStatus(buf []byte) (*Status, bool) {
	r := wire.NewReader(buf)
	state, ok := r.Bytes()
	if !ok || !utf8.Valid(state) {
		return nil, false
	}
	st, ok := parseState(string(state))
	if !ok {
		return nil, false
	}
	wakeAt, ok1 := r.U64()
	names, ok2 := r.Bytes()
	hasOutput, ok3 := r.U64()
	output, ok4 := r.Bytes()
	errText, ok5 := r.Bytes()
	positions, ok6 := r.U64()
	if !ok1 || !ok2 || !ok3 || !ok4 || !ok5 || !ok6 || !utf8.Valid(names) || !utf8.Valid(errText) {
		return nil, false
	}
	s := &Status{State: st, WakeAt: wakeAt, Error: string(errText), Positions: positions}
	for _, n := range strings.Split(string(names), "\n") {
		if n != "" {
			s.WaitingFor = append(s.WaitingFor, n)
		}
	}
	if hasOutput == 1 {
		s.Output = output
	}
	return s, true
}
