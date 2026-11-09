package workflow

import (
	"encoding/hex"
	"errors"
	"reflect"
	"testing"

	"github.com/wasmkeep/wasmkeep/sdks/go/call"
)

// The status encoding against the Rust SDK's golden bytes, then the
// client against a fake instance: the exact requests it sends and how it
// reads answers, statuses and failed calls, as the Rust client does.

// golden is the Rust SDK's encode_status of {waiting, wake_at 42,
// waiting_for [approved, paid], no output, no error, positions 3}.
const golden = "00000007" + "77616974696e67" + // "waiting"
	"000000000000002a" + // wake_at 42
	"0000000d" + "617070726f7665640a70616964" + // "approved\npaid"
	"0000000000000000" + // no output
	"00000000" + // output
	"00000000" + // error
	"0000000000000003" // positions

func TestStatusMatchesTheRustGoldenBytes(t *testing.T) {
	s := &Status{State: Waiting, WakeAt: 42, WaitingFor: []string{"approved", "paid"}, Positions: 3}
	if got := hex.EncodeToString(encodeStatus(s)); got != golden {
		t.Fatalf("got  %s\nwant %s", got, golden)
	}
	raw, _ := hex.DecodeString(golden)
	back, ok := decodeStatus(raw)
	if !ok || !reflect.DeepEqual(back, s) {
		t.Fatalf("decoded %+v %v", back, ok)
	}
}

func TestStatusRoundTripsOutputErrorAndEveryState(t *testing.T) {
	for _, s := range []*Status{
		{State: Completed, Output: []byte("shipped book"), Positions: 6},
		{State: Completed, Output: []byte{}, Positions: 1}, // an empty output is still an output
		{State: Failed, Error: "charge: out of attempts", Positions: 2},
		{State: Running},
		{State: Cancelled, Positions: 4},
	} {
		back, ok := decodeStatus(encodeStatus(s))
		if !ok || !reflect.DeepEqual(back, s) {
			t.Fatalf("%+v came back as %+v %v", s, back, ok)
		}
		if back.State.IsFinished() == (s.State == Running) {
			t.Fatalf("IsFinished(%s)", s.State)
		}
	}
}

func TestMalformedStatusesAreRefused(t *testing.T) {
	good := encodeStatus(&Status{State: Waiting, WaitingFor: []string{"x"}})
	bad := encodeStatus(&Status{State: "paused"})
	notUTF8 := encodeStatus(&Status{State: Failed, Error: "\xff"})
	for name, buf := range map[string][]byte{
		"empty":         {},
		"truncated":     good[:len(good)-1],
		"unknown state": bad,
		"not utf-8":     notUTF8,
	} {
		if s, ok := decodeStatus(buf); ok {
			t.Errorf("%s: decoded %+v", name, s)
		}
	}
}

type request struct {
	address, method, path string
	headers               [][2]string
	body                  []byte
}

type answer struct {
	status uint16
	body   []byte
	err    error
}

func fake(f func(r request) answer) (call.Caller, *[]request) {
	var calls []request
	return func(address, method, path string, headers [][2]string, body []byte) (uint16, []byte, error) {
		r := request{address, method, path, headers, append([]byte{}, body...)}
		calls = append(calls, r)
		a := f(r)
		return a.status, a.body, a.err
	}, &calls
}

func expectErr(t *testing.T, err error, kind *Error, message string) {
	t.Helper()
	var e *Error
	if !errors.Is(err, kind) || !errors.As(err, &e) {
		t.Fatalf("error %v, want kind %d", err, kind.Kind)
	}
	if e.Error() != message {
		t.Fatalf("message %q, want %q", e.Error(), message)
	}
}

func TestEachOperationSendsItsRequest(t *testing.T) {
	waiting := &Status{State: Waiting, WaitingFor: []string{"approved"}, Positions: 2}
	c, calls := fake(func(r request) answer {
		if r.path == "/wasmkeep-workflow/v1/event?name=617070726f766564" {
			return answer{status: 200, body: []byte{}}
		}
		return answer{status: 200, body: encodeStatus(waiting)}
	})
	orders := New("orders", c)
	if orders.Class() != "orders" {
		t.Fatal(orders.Class())
	}
	for _, s := range []func() (*Status, error){
		func() (*Status, error) { return orders.Start("o1", []byte("item=book")) },
		func() (*Status, error) { return orders.Status("o1") },
		func() (*Status, error) { return orders.Cancel("o1") },
	} {
		got, err := s()
		if err != nil || !reflect.DeepEqual(got, waiting) {
			t.Fatalf("%+v %v", got, err)
		}
	}
	if err := orders.SendEvent("o1", "approved", []byte("by ada")); err != nil {
		t.Fatal(err)
	}
	// An event name is hex in the query, so any name fits.
	if err := orders.SendEvent("o2", "é ?&", nil); err != nil {
		t.Fatal(err)
	}
	want := []request{
		{"orders/o1", "POST", "/wasmkeep-workflow/v1/start", nil, []byte("item=book")},
		{"orders/o1", "GET", "/wasmkeep-workflow/v1/status", nil, []byte{}},
		{"orders/o1", "POST", "/wasmkeep-workflow/v1/cancel", nil, []byte{}},
		{"orders/o1", "POST", "/wasmkeep-workflow/v1/event?name=617070726f766564", nil, []byte("by ada")},
		{"orders/o2", "POST", "/wasmkeep-workflow/v1/event?name=c3a9203f26", nil, []byte{}},
	}
	if !reflect.DeepEqual(*calls, want) {
		t.Fatalf("calls\n%+v\nwant\n%+v", *calls, want)
	}
}

func TestStatusesMapToErrors(t *testing.T) {
	var next answer
	c, _ := fake(func(request) answer { return next })
	orders := New("orders", c)
	ops := map[string]func() error{
		"start":  func() error { _, err := orders.Start("o", nil); return err },
		"status": func() error { _, err := orders.Status("o"); return err },
		"event":  func() error { return orders.SendEvent("o", "approved", nil) },
		"cancel": func() error { _, err := orders.Cancel("o"); return err },
	}
	for name, op := range ops {
		next = answer{status: 404, body: []byte("no such workflow instance")}
		expectErr(t, op(), ErrUnknown, "no such workflow instance")
		next = answer{status: 409, body: []byte("finished")}
		expectErr(t, op(), ErrFinished, "the workflow instance has finished")
		next = answer{status: 500, body: []byte("storage: disk full\n")}
		err := op()
		expectErr(t, err, ErrRefused, "refused (500): storage: disk full")
		var e *Error
		if errors.As(err, &e); e.Status != 500 {
			t.Fatalf("%s: status %d", name, e.Status)
		}
		next = answer{err: call.Failure("orders/o", "timeout", "")}
		expectErr(t, op(), ErrCall, "call to orders/o timed out")
	}
	// A reply that is not a status is refused, for the operations that
	// read one.
	next = answer{status: 200, body: []byte("not a status")}
	for _, name := range []string{"start", "status", "cancel"} {
		expectErr(t, ops[name](), ErrRefused, "refused (200): malformed status")
	}
	if err := ops["event"](); err != nil {
		t.Fatalf("an event's 200 has no body to read: %v", err)
	}
}
