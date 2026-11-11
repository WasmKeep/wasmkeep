// Package queue is queues with at-least-once delivery, built from one actor
// (ADR 0015): the producer and consumer side.
//
// A queue is an actor whose SQLite table holds its messages: the Rust
// SDK's queue, deployed from examples/jobs/queue (its settings, such as
// the push consumer, are code in that class). This package speaks its
// protocol (`/wasmkeep-queue/v1`) through the app's [call.Caller]:
//
//   - producers Send (returns once the message is durable);
//   - pull consumers Lease a batch, then Ack or Retry each message;
//   - push consumers answer deliveries with [HandleDelivery].
//
// A message may be delivered more than once (a lost ack, a lease that ran
// out, a failover), with the same ID: record the ids you handled when a
// duplicate would matter.
//
//	jobs := queue.New("queue/jobs", wasmkeep.Caller)
//	id, err := jobs.Send([]byte("resize 42"))
package queue

import (
	"strconv"
	"strings"

	"github.com/wasmkeep/wasmkeep/sdks/go/call"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

const (
	prefix   = "/wasmkeep-queue/v1"
	hdrDelay = "x-wasmkeep-queue-delay-ms"
)

// DeliverPath is the path a push delivery arrives on, at the consumer.
const DeliverPath = "/wasmkeep-queue/v1/deliver"

// CallerHeader is the header the host sets on calls between actors: the
// caller's address. A delivery without it came from outside.
const CallerHeader = "x-wasmkeep-caller"

// MaxBatch is how many messages one send may carry.
const MaxBatch = 1000

// Kind is why a queue operation failed.
type Kind uint8

const (
	// Storage: the queue's database refused the operation.
	Storage Kind = iota + 1
	// Call: the call to the queue failed. For a send, the message may or
	// may not be queued.
	Call
	// Refused: the queue answered with an unexpected status.
	Refused
)

// Error is a failed queue operation. Match a kind with errors.Is and the
// sentinels ([ErrCall], ...), or read the fields with errors.As.
type Error struct {
	Kind Kind
	// Status is the queue's status, for Refused.
	Status uint16
	// Detail is the call's error (Call), the database's message
	// (Storage), or the queue's answer (Refused).
	Detail string
}

// The sentinels, one per kind: errors.Is(err, queue.ErrCall) holds for
// any *Error of that kind.
var (
	ErrStorage = &Error{Kind: Storage}
	ErrCall    = &Error{Kind: Call}
	ErrRefused = &Error{Kind: Refused}
)

// Error is the Rust SDK's message for the same error.
func (e *Error) Error() string {
	switch e.Kind {
	case Storage:
		return "storage: " + e.Detail
	case Call:
		return e.Detail
	}
	return "queue answered " + strconv.FormatUint(uint64(e.Status), 10) + ": " + e.Detail
}

// Is matches any error of the same kind.
func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Kind == e.Kind
}

func badReply() error {
	return &Error{Kind: Refused, Status: 200, Detail: "malformed reply from queue"}
}

// Client reaches one queue actor.
type Client struct {
	address string
	call    call.Caller
}

// New is the queue at address (`class/name`), reached through c.
func New(address string, c call.Caller) *Client {
	return &Client{address: address, call: c}
}

// Address is the queue's address.
func (c *Client) Address() string { return c.address }

func (c *Client) do(method, path string, headers [][2]string, body []byte) ([]byte, error) {
	status, reply, err := c.call(c.address, method, path, headers, body)
	if err != nil {
		return nil, &Error{Kind: Call, Detail: err.Error()}
	}
	if status != 200 {
		return nil, &Error{Kind: Refused, Status: status, Detail: wire.Lossy(reply)}
	}
	return reply, nil
}

// Send queues body; returns its id once it is durable.
func (c *Client) Send(body []byte) (uint64, error) {
	return c.SendDelayed(body, 0)
}

// SendDelayed queues body, visible after delayMs.
func (c *Client) SendDelayed(body []byte, delayMs uint64) (uint64, error) {
	ids, err := c.sendBatch([][]byte{body}, delayMs)
	if err != nil {
		return 0, err
	}
	return ids[0], nil
}

// SendBatch queues several messages (at most [MaxBatch]: the queue refuses
// more) in one durable write; returns their ids in order. All or none are
// queued.
func (c *Client) SendBatch(bodies [][]byte) ([]uint64, error) {
	return c.sendBatch(bodies, 0)
}

func (c *Client) sendBatch(bodies [][]byte, delayMs uint64) ([]uint64, error) {
	headers := [][2]string{{hdrDelay, strconv.FormatUint(delayMs, 10)}}
	reply, err := c.do("POST", prefix+"/send", headers, encodeBodies(bodies))
	if err != nil {
		return nil, err
	}
	ids, ok := decodeCounts(reply)
	if !ok || len(ids) != len(bodies) {
		return nil, badReply()
	}
	return ids, nil
}

// Lease leases up to max visible messages for visibilityMs.
func (c *Client) Lease(max uint32, visibilityMs uint64) ([]Message, error) {
	path := prefix + "/lease?max=" + strconv.FormatUint(uint64(max), 10) +
		"&visibility_ms=" + strconv.FormatUint(visibilityMs, 10)
	reply, err := c.do("POST", path, [][2]string{}, []byte{})
	if err != nil {
		return nil, err
	}
	messages, ok := decodeMessages(reply)
	if !ok {
		return nil, badReply()
	}
	return messages, nil
}

func (c *Client) settleOne(m Message, o Outcome) (bool, error) {
	flags, err := c.Settle([]Settlement{{ID: m.ID, Attempt: m.Attempt, Outcome: o}})
	if err != nil {
		return false, err
	}
	return len(flags) > 0 && flags[0], nil
}

// Ack deletes a leased message (only its ID and Attempt are read). false
// if this lease is no longer current (it ran out and the message was
// leased again, or was settled).
func (c *Client) Ack(m Message) (bool, error) {
	return c.settleOne(m, Ack())
}

// Retry makes a leased message visible again after the queue's backoff.
// false if this lease is no longer current.
func (c *Client) Retry(m Message) (bool, error) {
	return c.settleOne(m, Retry())
}

// RetryAfter makes a leased message visible again after delayMs. false if
// this lease is no longer current.
func (c *Client) RetryAfter(m Message, delayMs uint64) (bool, error) {
	return c.settleOne(m, RetryAfter(delayMs))
}

// Settle settles several leased messages in one call; one flag per item,
// true where the lease was current.
func (c *Client) Settle(items []Settlement) ([]bool, error) {
	reply, err := c.do("POST", prefix+"/settle", [][2]string{}, encodeSettlements(items))
	if err != nil {
		return nil, err
	}
	counts, ok := decodeCounts(reply)
	if !ok {
		return nil, badReply()
	}
	flags := make([]bool, len(counts))
	for i, f := range counts {
		flags[i] = f == 1
	}
	return flags, nil
}

// Stats counts the queue's messages: ready, delayed, in flight and dead.
func (c *Client) Stats() (Stats, error) {
	reply, err := c.do("GET", prefix+"/stats", [][2]string{}, []byte{})
	if err != nil {
		return Stats{}, err
	}
	counts, ok := decodeCounts(reply)
	if !ok || len(counts) != 4 {
		return Stats{}, badReply()
	}
	return Stats{Ready: counts[0], Delayed: counts[1], InFlight: counts[2], Dead: counts[3]}, nil
}

// DeadLetters returns the oldest limit dead-lettered messages.
func (c *Client) DeadLetters(limit uint32) ([]DeadLetter, error) {
	reply, err := c.do("GET", prefix+"/dead?limit="+strconv.FormatUint(uint64(limit), 10), [][2]string{}, []byte{})
	if err != nil {
		return nil, err
	}
	items, ok := decodeDead(reply)
	if !ok {
		return nil, badReply()
	}
	return items, nil
}

// IsDelivery reports whether a request (its method and path with query)
// is a push delivery from a queue: answer it with [HandleDelivery].
func IsDelivery(method, pathWithQuery string) bool {
	return method == "POST" && pathWithQuery == DeliverPath
}

// CallerFrom is the value of the `x-wasmkeep-caller` header in headers
// (the first one, matched case-insensitively), or "".
func CallerFrom(headers [][2]string) string {
	for _, h := range headers {
		if strings.EqualFold(h[0], CallerHeader) {
			return h[1]
		}
	}
	return ""
}

// Reply is the response to give a push delivery.
type Reply struct {
	Status      uint16
	ContentType string
	Body        []byte
}

// HandleDelivery answers a push delivery. caller is the request's
// `x-wasmkeep-caller` header ("" if it has none) and body its body. handle
// gets the sending queue's address (`tenant/class/name`) and each
// message, and returns its outcome. Deliveries from outside clients
// (without the header) get 403, a malformed body 400; otherwise 200 with
// one settlement per message. If handle panics the event fails, and the
// queue retries the whole batch with its backoff. Record handled ids in
// this actor's database, in the same event, when a duplicate delivery
// would matter.
func HandleDelivery(caller string, body []byte, handle func(queue string, m Message) Outcome) Reply {
	if caller == "" {
		return Reply{Status: 403, ContentType: "text/plain", Body: []byte("deliveries come from queue actors\n")}
	}
	messages, ok := decodeMessages(body)
	if !ok {
		return Reply{Status: 400, ContentType: "text/plain", Body: []byte("malformed delivery\n")}
	}
	outcomes := make([]Settlement, len(messages))
	for i, m := range messages {
		outcomes[i] = Settlement{ID: m.ID, Attempt: m.Attempt, Outcome: handle(caller, m)}
	}
	return Reply{Status: 200, ContentType: "application/octet-stream", Body: encodeSettlements(outcomes)}
}
