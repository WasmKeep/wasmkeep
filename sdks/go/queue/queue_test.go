package queue

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/wasmkeep/wasmkeep/sdks/go/call"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

// The client against a fake queue: the exact requests it sends and how it
// reads answers, statuses and failed calls, as the Rust and TypeScript
// clients do.

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

func ok(body []byte) answer { return answer{status: 200, body: body} }

func expectErr(t *testing.T, err error, kind *Error, message string, s uint16) {
	t.Helper()
	var e *Error
	if !errors.Is(err, kind) || !errors.As(err, &e) {
		t.Fatalf("error %v, want kind %d", err, kind.Kind)
	}
	if message != "" && e.Error() != message {
		t.Fatalf("message %q, want %q", e.Error(), message)
	}
	if s != 0 && e.Status != s {
		t.Fatalf("status %d, want %d", e.Status, s)
	}
}

func TestSendSendDelayedAndSendBatch(t *testing.T) {
	var answerWith *answer
	c, calls := fake(func(r request) answer {
		if answerWith != nil {
			return *answerWith
		}
		n := binary.BigEndian.Uint64(r.body)
		ids := make([]uint64, n)
		for i := range ids {
			ids[i] = 10 + uint64(i)
		}
		return ok(encodeCounts(ids))
	})
	jobs := New("queue/jobs", c)
	if id, err := jobs.Send([]byte("resize 42")); err != nil || id != 10 {
		t.Fatal(id, err)
	}
	var w wire.Writer
	want := request{
		address: "queue/jobs",
		method:  "POST",
		path:    "/wasmkeep-queue/v1/send",
		headers: [][2]string{{"x-wasmkeep-queue-delay-ms", "0"}},
		body:    w.U64(1).Bytes([]byte("resize 42")).Finish(),
	}
	if !reflect.DeepEqual((*calls)[0], want) {
		t.Fatalf("%+v", (*calls)[0])
	}
	if id, err := jobs.SendDelayed([]byte("later"), 3_600_000); err != nil || id != 10 {
		t.Fatal(id, err)
	}
	if got := (*calls)[1].headers; !reflect.DeepEqual(got, [][2]string{{"x-wasmkeep-queue-delay-ms", "3600000"}}) {
		t.Fatal(got)
	}
	ids, err := jobs.SendBatch([][]byte{[]byte("a"), {1}, nil})
	if err != nil || !reflect.DeepEqual(ids, []uint64{10, 11, 12}) {
		t.Fatal(ids, err)
	}
	var b wire.Writer
	if !reflect.DeepEqual((*calls)[2].body, b.U64(3).Bytes([]byte("a")).Bytes([]byte{1}).Bytes(nil).Finish()) {
		t.Fatal(hex.EncodeToString((*calls)[2].body))
	}

	answerWith = &answer{status: 200, body: encodeCounts([]uint64{1})}
	_, err = jobs.SendBatch([][]byte{[]byte("a"), []byte("b")})
	expectErr(t, err, ErrRefused, "queue answered 200: malformed reply from queue", 200)
	answerWith = &answer{status: 200, body: encodeCounts(nil)}
	_, err = jobs.Send([]byte("x"))
	expectErr(t, err, ErrRefused, "", 200)
	answerWith = &answer{status: 413, body: []byte("more than 1000 messages\n")}
	_, err = jobs.Send([]byte("x"))
	expectErr(t, err, ErrRefused, "queue answered 413: more than 1000 messages", 413)
	answerWith = &answer{err: call.Failure("queue/jobs", "timeout", "")}
	_, err = jobs.Send([]byte("x"))
	expectErr(t, err, ErrCall, "call to queue/jobs timed out", 0)
	if errors.Is(err, ErrRefused) {
		t.Fatal("errors.Is matches only its own kind")
	}
}

func TestLeaseAckRetryAndSettle(t *testing.T) {
	leased := []Message{{ID: 4, Attempt: 1, EnqueuedAt: 1000, Body: []byte("hello")}}
	c, calls := fake(func(r request) answer {
		if strings.Contains(r.path, "/lease") {
			return ok(encodeMessages(leased))
		}
		items, _ := decodeSettlements(r.body)
		flags := make([]uint64, len(items))
		for i, s := range items {
			if s.Attempt == 1 {
				flags[i] = 1
			}
		}
		return ok(encodeCounts(flags))
	})
	inbox := New("queue/inbox", c)
	got, err := inbox.Lease(10, 30_000)
	if err != nil || !reflect.DeepEqual(got, leased) {
		t.Fatal(got, err)
	}
	want := request{"queue/inbox", "POST", "/wasmkeep-queue/v1/lease?max=10&visibility_ms=30000", [][2]string{}, []byte{}}
	if !reflect.DeepEqual((*calls)[0], want) {
		t.Fatalf("%+v", (*calls)[0])
	}
	if acked, err := inbox.Ack(leased[0]); err != nil || !acked {
		t.Fatal(acked, err)
	}
	if p := (*calls)[1].path; p != "/wasmkeep-queue/v1/settle" {
		t.Fatal(p)
	}
	if h := hex.EncodeToString((*calls)[1].body); h != "0000000000000001"+"0000000000000004"+"0000000000000001"+strings.Repeat("00", 16) {
		t.Fatal(h)
	}
	if acked, _ := inbox.Ack(Message{ID: 4, Attempt: 2}); acked {
		t.Fatal("a stale lease")
	}
	if retried, err := inbox.Retry(leased[0]); err != nil || !retried {
		t.Fatal(retried, err)
	}
	if s, _ := decodeSettlements((*calls)[3].body); !reflect.DeepEqual(s, []Settlement{{ID: 4, Attempt: 1, Outcome: Retry()}}) {
		t.Fatal(s)
	}
	inbox.RetryAfter(leased[0], 250)
	if s, _ := decodeSettlements((*calls)[4].body); !reflect.DeepEqual(s, []Settlement{{ID: 4, Attempt: 1, Outcome: RetryAfter(250)}}) {
		t.Fatal(s)
	}
	flags, err := inbox.Settle([]Settlement{{ID: 1, Attempt: 1, Outcome: Ack()}, {ID: 2, Attempt: 2, Outcome: Retry()}})
	if err != nil || !reflect.DeepEqual(flags, []bool{true, false}) {
		t.Fatal(flags, err)
	}
}

func TestStatsAndDeadLetters(t *testing.T) {
	dead := []DeadLetter{{Message: Message{ID: 2, Attempt: 3, EnqueuedAt: 5, Body: []byte("fail")}, FailedAt: 9, Reason: "attempts exhausted"}}
	counts := []uint64{1, 2, 3, 4}
	c, calls := fake(func(r request) answer {
		if strings.HasSuffix(r.path, "/stats") {
			return ok(encodeCounts(counts))
		}
		return ok(encodeDead(dead))
	})
	q := New("queue/jobs", c)
	if s, err := q.Stats(); err != nil || s != (Stats{Ready: 1, Delayed: 2, InFlight: 3, Dead: 4}) {
		t.Fatal(s, err)
	}
	if d, err := q.DeadLetters(100); err != nil || !reflect.DeepEqual(d, dead) {
		t.Fatal(d, err)
	}
	for i, want := range [][2]string{{"GET", "/wasmkeep-queue/v1/stats"}, {"GET", "/wasmkeep-queue/v1/dead?limit=100"}} {
		if got := [2]string{(*calls)[i].method, (*calls)[i].path}; got != want {
			t.Fatal(got)
		}
	}
	counts = []uint64{1, 2, 3}
	_, err := q.Stats()
	expectErr(t, err, ErrRefused, "", 200)
}

func TestHandleDeliveryAnswersOneOutcomePerMessage(t *testing.T) {
	batch := []Message{
		{ID: 1, Attempt: 1, EnqueuedAt: 10, Body: []byte("resize 42")},
		{ID: 2, Attempt: 3, EnqueuedAt: 11, Body: []byte("fail")},
	}
	if !IsDelivery("POST", DeliverPath) || IsDelivery("GET", DeliverPath) || IsDelivery("POST", DeliverPath+"?x=1") {
		t.Fatal("IsDelivery")
	}
	var seen []string
	r := HandleDelivery("default/queue/jobs", encodeMessages(batch), func(from string, m Message) Outcome {
		seen = append(seen, from+" "+string(rune('0'+m.ID)))
		if strings.HasPrefix(string(m.Body), "fail") {
			return Retry()
		}
		return Ack()
	})
	if !reflect.DeepEqual(seen, []string{"default/queue/jobs 1", "default/queue/jobs 2"}) {
		t.Fatal(seen)
	}
	if r.Status != 200 || r.ContentType != "application/octet-stream" {
		t.Fatal(r)
	}
	if s, ok := decodeSettlements(r.Body); !ok || !reflect.DeepEqual(s, []Settlement{
		{ID: 1, Attempt: 1, Outcome: Ack()}, {ID: 2, Attempt: 3, Outcome: Retry()},
	}) {
		t.Fatal(s, ok)
	}
	// An empty batch is answered with no settlements.
	if r := HandleDelivery("q", encodeMessages(nil), nil); r.Status != 200 || hex.EncodeToString(r.Body) != "0000000000000000" {
		t.Fatal(r)
	}

	called := false
	handle := func(string, Message) Outcome { called = true; return Ack() }
	outside := HandleDelivery("", encodeMessages(batch), handle)
	if !reflect.DeepEqual(outside, Reply{403, "text/plain", []byte("deliveries come from queue actors\n")}) {
		t.Fatal(outside)
	}
	garbage := HandleDelivery("q", []byte{1, 2, 3}, handle)
	if !reflect.DeepEqual(garbage, Reply{400, "text/plain", []byte("malformed delivery\n")}) {
		t.Fatal(garbage)
	}
	if called {
		t.Fatal("the handler ran for a refused delivery")
	}

	// The caller header is looked up case-insensitively.
	headers := [][2]string{{"content-type", "x"}, {"X-WasmKeep-Caller", "default/queue/q"}, {"x-wasmkeep-caller", "second"}}
	if got := CallerFrom(headers); got != "default/queue/q" {
		t.Fatal(got)
	}
	if CallerFrom(nil) != "" {
		t.Fatal("no header")
	}
}
