package queue

import (
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

func TestQueueFramesRoundTrip(t *testing.T) {
	messages := []Message{
		{ID: 1, Attempt: 2, EnqueuedAt: 3, Body: []byte("x")},
		{ID: ^uint64(0), Attempt: 0xffff_ffff, EnqueuedAt: 0, Body: []byte{}},
	}
	if got, ok := decodeMessages(encodeMessages(messages)); !ok || !reflect.DeepEqual(got, messages) {
		t.Fatal(got, ok)
	}
	items := []Settlement{
		{ID: 1, Attempt: 1, Outcome: Ack()},
		{ID: 2, Attempt: 3, Outcome: Retry()},
		{ID: 3, Attempt: 1, Outcome: RetryAfter(50)},
	}
	if got, ok := decodeSettlements(encodeSettlements(items)); !ok || !reflect.DeepEqual(got, items) {
		t.Fatal(got, ok)
	}
	if got, ok := decodeCounts(encodeCounts([]uint64{4, 5})); !ok || !reflect.DeepEqual(got, []uint64{4, 5}) {
		t.Fatal(got, ok)
	}
	if got, ok := decodeBodies(encodeBodies([][]byte{[]byte("a"), {}})); !ok || !reflect.DeepEqual(got, [][]byte{[]byte("a"), {}}) {
		t.Fatal(got, ok)
	}
	dead := []DeadLetter{{Message: messages[0], FailedAt: 9, Reason: "attempts exhausted"}}
	if got, ok := decodeDead(encodeDead(dead)); !ok || !reflect.DeepEqual(got, dead) {
		t.Fatal(got, ok)
	}
	// Golden bytes: a message as Rust frames it.
	h := hex.EncodeToString(encodeMessages(messages[:1]))
	if want := "0000000000000001" + "0000000000000001" + "0000000000000002" + "0000000000000003" + "0000000178"; h != want {
		t.Fatal(h)
	}
}

func TestSettlementsEncodeAsRustDoes(t *testing.T) {
	one := func(o Outcome) string {
		return hex.EncodeToString(encodeSettlements([]Settlement{{ID: 5, Attempt: 2, Outcome: o}}))
	}
	head := "0000000000000001" + "0000000000000005" + "0000000000000002"
	for name, c := range map[string]struct {
		o    Outcome
		want string
	}{
		"ack":         {Ack(), head + "0000000000000000" + "0000000000000000"},
		"zero value":  {Outcome{}, head + "0000000000000000" + "0000000000000000"},
		"retry":       {Retry(), head + "0000000000000001" + "ffffffffffffffff"},
		"retry after": {RetryAfter(50), head + "0000000000000001" + "0000000000000032"},
	} {
		if got := one(c.o); got != c.want {
			t.Errorf("%s: %s, want %s", name, got, c.want)
		}
	}
	if !Ack().IsAck() || Retry().IsAck() {
		t.Fatal("IsAck")
	}
	if d, ok := RetryAfter(7).RetryDelay(); !ok || d != 7 {
		t.Fatal("RetryDelay")
	}
	if _, ok := Retry().RetryDelay(); ok {
		t.Fatal("Retry has no delay of its own")
	}
}

func TestMalformedFramesAreRefused(t *testing.T) {
	if _, ok := decodeMessages([]byte{}); ok {
		t.Fatal("empty")
	}
	// A count promising more than the buffer holds.
	one := encodeCounts([]uint64{7})
	one[7] = 2
	if _, ok := decodeCounts(one); ok {
		t.Fatal("short")
	}
	// Trailing bytes.
	if _, ok := decodeCounts(append(encodeCounts([]uint64{1}), 0)); ok {
		t.Fatal("trailing")
	}
	var w wire.Writer
	// An unknown outcome kind.
	if _, ok := decodeSettlements(w.U64(1).U64(1).U64(1).U64(9).U64(0).Finish()); ok {
		t.Fatal("kind 9")
	}
	// An attempt past u32::MAX (Rust: `u32::try_from` fails).
	var a wire.Writer
	if _, ok := decodeMessages(a.U64(1).U64(1).U64(0x1_0000_0000).U64(0).Bytes([]byte("x")).Finish()); ok {
		t.Fatal("attempt")
	}
	// A dead letter's reason must be UTF-8 (Rust: `String::from_utf8`).
	var d wire.Writer
	if _, ok := decodeDead(d.U64(1).U64(1).U64(1).U64(0).Bytes([]byte("x")).U64(9).Bytes([]byte{0xff}).Finish()); ok {
		t.Fatal("reason")
	}
	// A huge count with no items fails rather than allocating.
	var h wire.Writer
	if _, ok := decodeMessages(h.U64(1 << 62).Finish()); ok {
		t.Fatal("huge count")
	}
}
