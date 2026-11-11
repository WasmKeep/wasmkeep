// Package workflow starts and follows durable workflows (ADR 0019): the
// client side.
//
// A workflow instance is an actor (`<class>/<id>`) whose SQLite tables
// record its steps, moved forward by its alarm: the Rust SDK's runner,
// deployed from a class such as examples/workflows/orders. This package
// speaks its protocol (`/wasmkeep-workflow/v1`) through the app's
// [call.Caller]:
//
//	orders := workflow.New("orders", wasmkeep.Caller)
//	status, err := orders.Start("order-42", []byte("item=book"))
//	err = orders.SendEvent("order-42", "approved", nil)
//	status, err = orders.Status("order-42")
//	if errors.Is(err, workflow.ErrUnknown) { /* never started, or gone */ }
//
// Start only records: the instance runs in its own event right after,
// never inside the caller's call.
package workflow

import (
	"strconv"

	"github.com/wasmkeep/wasmkeep/sdks/go/call"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

const prefix = "/wasmkeep-workflow/v1"

// Kind is why a workflow operation failed.
type Kind uint8

const (
	// Unknown: no instance with that id (never started, or deleted after
	// its retention).
	Unknown Kind = iota + 1
	// Finished: the instance has finished, so it takes no more events.
	Finished
	// Call: the call failed (the instance may or may not have recorded
	// it).
	Call
	// Refused: the instance answered with an unexpected status, or a
	// malformed status.
	Refused
)

// Error is a failed workflow operation. Match a kind with errors.Is and
// the sentinels ([ErrUnknown], ...), or read the fields with errors.As.
type Error struct {
	Kind Kind
	// Status is the instance's status, for Refused.
	Status uint16
	// Detail is the call's error (Call) or the instance's answer
	// (Refused).
	Detail string
}

// The sentinels, one per kind: errors.Is(err, workflow.ErrUnknown) holds
// for any *Error of that kind.
var (
	ErrUnknown  = &Error{Kind: Unknown}
	ErrFinished = &Error{Kind: Finished}
	ErrCall     = &Error{Kind: Call}
	ErrRefused  = &Error{Kind: Refused}
)

// Error is the Rust SDK's message for the same error.
func (e *Error) Error() string {
	switch e.Kind {
	case Unknown:
		return "no such workflow instance"
	case Finished:
		return "the workflow instance has finished"
	case Call:
		return e.Detail
	}
	return "refused (" + strconv.FormatUint(uint64(e.Status), 10) + "): " + e.Detail
}

// Is matches any error of the same kind.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Kind == e.Kind
}

// Client starts and queries instances of one workflow class.
type Client struct {
	class string
	call  call.Caller
}

// New is instances of class (the workflow class's name), reached
// through c.
func New(class string, c call.Caller) *Client {
	return &Client{class: class, call: c}
}

// Class is the workflow class's name.
func (c *Client) Class() string { return c.class }

func (c *Client) do(id, method, path string, body []byte) ([]byte, error) {
	if body == nil {
		body = []byte{}
	}
	status, reply, err := c.call(c.class+"/"+id, method, path, nil, body)
	if err != nil {
		return nil, &Error{Kind: Call, Detail: err.Error()}
	}
	switch status {
	case 200:
		return reply, nil
	case 404:
		return nil, &Error{Kind: Unknown}
	case 409:
		return nil, &Error{Kind: Finished}
	}
	return nil, &Error{Kind: Refused, Status: status, Detail: wire.Lossy(reply)}
}

func (c *Client) status(id, method, path string, body []byte) (*Status, error) {
	reply, err := c.do(id, method, path, body)
	if err != nil {
		return nil, err
	}
	s, ok := decodeStatus(reply)
	if !ok {
		return nil, &Error{Kind: Refused, Status: 200, Detail: "malformed status"}
	}
	return s, nil
}

// Start starts instance id with input, or returns its status if it exists
// (a start is idempotent while the instance is kept). It runs in its own
// event, soon after: this call only records it.
func (c *Client) Start(id string, input []byte) (*Status, error) {
	return c.status(id, "POST", prefix+"/start", input)
}

// Status is the instance's status; an error of kind Unknown if there is
// none.
func (c *Client) Status(id string) (*Status, error) {
	return c.status(id, "GET", prefix+"/status", nil)
}

// SendEvent delivers event name with payload: recorded durably, then the
// instance runs. Unknown if it was never started, Finished if it has
// finished.
func (c *Client) SendEvent(id, name string, payload []byte) error {
	_, err := c.do(id, "POST", prefix+"/event?name="+wire.Hex([]byte(name)), payload)
	return err
}

// Cancel cancels the instance: it runs no further step (one running now
// finishes first). Returns its status; a finished one is unchanged.
func (c *Client) Cancel(id string) (*Status, error) {
	return c.status(id, "POST", prefix+"/cancel", nil)
}
