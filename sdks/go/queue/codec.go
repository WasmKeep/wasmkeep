package queue

import (
	"unicode/utf8"

	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

const u64Max = ^uint64(0)

// Message is a leased or delivered message.
type Message struct {
	// ID is unique within the queue, never reused, the same on every
	// delivery.
	ID uint64
	// Attempt is which delivery this is, from 1. Acks and retries name it,
	// so one for an earlier lease of a message leased again is ignored.
	Attempt uint32
	// EnqueuedAt is when it was sent (ms since the Unix epoch).
	EnqueuedAt uint64
	// Body is what was sent.
	Body []byte
}

type outcomeKind uint8

const (
	outcomeAck outcomeKind = iota
	outcomeRetry
	outcomeRetryAfter
)

// Outcome is what a consumer did with a message: [Ack] (handled: delete
// it, the zero value), [Retry] (deliver it again after the queue's
// backoff) or [RetryAfter] (deliver it again after a delay). A retry of a
// message that had all its attempts dead-letters it.
type Outcome struct {
	kind    outcomeKind
	delayMs uint64
}

// Ack: the message was handled; delete it.
func Ack() Outcome { return Outcome{} }

// Retry: deliver the message again after the queue's backoff.
func Retry() Outcome { return Outcome{kind: outcomeRetry} }

// RetryAfter: deliver the message again after delayMs.
func RetryAfter(delayMs uint64) Outcome { return Outcome{kind: outcomeRetryAfter, delayMs: delayMs} }

// IsAck reports whether o is [Ack].
func (o Outcome) IsAck() bool { return o.kind == outcomeAck }

// RetryDelay returns the delay of a retry: (delay, true) for [RetryAfter],
// (0, false) for [Retry] (the queue's backoff) and [Ack].
func (o Outcome) RetryDelay() (uint64, bool) {
	return o.delayMs, o.kind == outcomeRetryAfter
}

// Settlement is a lease (ID, Attempt) and its outcome.
type Settlement struct {
	ID      uint64
	Attempt uint32
	Outcome Outcome
}

// Stats counts messages by state.
type Stats struct {
	// Ready: visible now; the next lease or delivery takes them.
	Ready uint64
	// Delayed: sent with a delay that has not passed.
	Delayed uint64
	// InFlight: leased and not yet acked, or waiting for a retry.
	InFlight uint64
	// Dead: in the dead-letter table.
	Dead uint64
}

// DeadLetter is a dead-lettered message.
type DeadLetter struct {
	// Message is the message as it was last delivered.
	Message Message
	// FailedAt is when it was dead-lettered (ms since the Unix epoch).
	FailedAt uint64
	// Reason is why: attempts exhausted, or the last delivery error.
	Reason string
}

func readMessage(r *wire.Reader) (Message, bool) {
	id, ok1 := r.U64()
	attempt, ok2 := r.U64()
	enqueued, ok3 := r.U64()
	body, ok4 := r.Bytes()
	// Rust's `u32::try_from` on the attempt.
	if !ok1 || !ok2 || !ok3 || !ok4 || attempt > 0xffff_ffff {
		return Message{}, false
	}
	return Message{ID: id, Attempt: uint32(attempt), EnqueuedAt: enqueued, Body: body}, true
}

func writeMessage(w *wire.Writer, m Message) {
	w.U64(m.ID).U64(uint64(m.Attempt)).U64(m.EnqueuedAt).Bytes(m.Body)
}

// count reads the item count of a list, then calls item that many times;
// false if one is malformed or bytes are left over.
func count(buf []byte, item func(r *wire.Reader) bool) bool {
	r := wire.NewReader(buf)
	n, ok := r.U64()
	if !ok {
		return false
	}
	for i := uint64(0); i < n; i++ {
		if !item(r) {
			return false
		}
	}
	return r.IsEmpty()
}

func encodeMessages(messages []Message) []byte {
	var w wire.Writer
	w.U64(uint64(len(messages)))
	for _, m := range messages {
		writeMessage(&w, m)
	}
	return w.Finish()
}

func decodeMessages(buf []byte) ([]Message, bool) {
	out := []Message{}
	ok := count(buf, func(r *wire.Reader) bool {
		m, ok := readMessage(r)
		out = append(out, m)
		return ok
	})
	return out, ok
}

// encodeSettlements writes kind 0 for an ack, kind 1 for a retry, with
// u64::MAX in the delay slot meaning "the queue's backoff".
func encodeSettlements(items []Settlement) []byte {
	var w wire.Writer
	w.U64(uint64(len(items)))
	for _, s := range items {
		var kind, delay uint64
		switch s.Outcome.kind {
		case outcomeRetry:
			kind, delay = 1, u64Max
		case outcomeRetryAfter:
			kind, delay = 1, s.Outcome.delayMs
		}
		w.U64(s.ID).U64(uint64(s.Attempt)).U64(kind).U64(delay)
	}
	return w.Finish()
}

func decodeSettlements(buf []byte) ([]Settlement, bool) {
	out := []Settlement{}
	ok := count(buf, func(r *wire.Reader) bool {
		id, ok1 := r.U64()
		attempt, ok2 := r.U64()
		kind, ok3 := r.U64()
		delay, ok4 := r.U64()
		if !ok1 || !ok2 || !ok3 || !ok4 || attempt > 0xffff_ffff {
			return false
		}
		var o Outcome
		switch {
		case kind == 0:
			o = Ack()
		case kind == 1 && delay == u64Max:
			o = Retry()
		case kind == 1:
			o = RetryAfter(delay)
		default:
			return false
		}
		out = append(out, Settlement{ID: id, Attempt: uint32(attempt), Outcome: o})
		return true
	})
	return out, ok
}

func encodeCounts(counts []uint64) []byte {
	var w wire.Writer
	w.U64(uint64(len(counts)))
	for _, c := range counts {
		w.U64(c)
	}
	return w.Finish()
}

func decodeCounts(buf []byte) ([]uint64, bool) {
	out := []uint64{}
	ok := count(buf, func(r *wire.Reader) bool {
		v, ok := r.U64()
		out = append(out, v)
		return ok
	})
	return out, ok
}

func encodeBodies(bodies [][]byte) []byte {
	var w wire.Writer
	w.U64(uint64(len(bodies)))
	for _, b := range bodies {
		w.Bytes(b)
	}
	return w.Finish()
}

func decodeBodies(buf []byte) ([][]byte, bool) {
	out := [][]byte{}
	ok := count(buf, func(r *wire.Reader) bool {
		b, ok := r.Bytes()
		out = append(out, b)
		return ok
	})
	return out, ok
}

func encodeDead(items []DeadLetter) []byte {
	var w wire.Writer
	w.U64(uint64(len(items)))
	for _, d := range items {
		writeMessage(&w, d.Message)
		w.U64(d.FailedAt).Bytes([]byte(d.Reason))
	}
	return w.Finish()
}

func decodeDead(buf []byte) ([]DeadLetter, bool) {
	out := []DeadLetter{}
	ok := count(buf, func(r *wire.Reader) bool {
		m, ok1 := readMessage(r)
		failed, ok2 := r.U64()
		reason, ok3 := r.Bytes()
		// Rust's `String::from_utf8`: invalid UTF-8 is malformed.
		if !ok1 || !ok2 || !ok3 || !utf8.Valid(reason) {
			return false
		}
		out = append(out, DeadLetter{Message: m, FailedAt: failed, Reason: string(reason)})
		return true
	})
	return out, ok
}
