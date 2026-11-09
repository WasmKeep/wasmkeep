package kv

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/wasmkeep/wasmkeep/sdks/go/call"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

// The client against fake shards: the exact requests it sends (address,
// method, path, headers, body) and how it reads answers, statuses and
// failed calls, as the Rust and TypeScript clients do.

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

// fake records every call and answers with f.
func fake(f func(r request) answer) (call.Caller, *[]request) {
	var calls []request
	return func(address, method, path string, headers [][2]string, body []byte) (uint16, []byte, error) {
		r := request{address, method, path, headers, append([]byte{}, body...)}
		calls = append(calls, r)
		a := f(r)
		return a.status, a.body, a.err
	}, &calls
}

func ok(body []byte) answer               { return answer{status: 200, body: body} }
func status(s uint16, text string) answer { return answer{status: s, body: []byte(text)} }
func u64(v uint64) []byte {
	var w wire.Writer
	return w.U64(v).Finish()
}

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

func TestPutPutWithAndCompareAndSetSendWhatRustSends(t *testing.T) {
	c, calls := fake(func(request) answer { return ok(u64(7)) })
	store := New("kv", 8, c)
	if v, err := store.Put("user:42", []byte("Ada")); err != nil || v != 7 {
		t.Fatal(v, err)
	}
	want := request{
		address: "kv/shard-2", // shard_for("user:42", 8) in Rust
		method:  "PUT",
		path:    "/wasmkeep-kv/v1/k/" + wire.Hex([]byte("user:42")),
		headers: [][2]string{{"x-wasmkeep-kv-expect", "any"}, {"x-wasmkeep-kv-durable", "1"}},
		body:    []byte("Ada"),
	}
	if !reflect.DeepEqual((*calls)[0], want) {
		t.Fatalf("%+v", (*calls)[0])
	}
	if _, err := store.PutWith("user:42", []byte{0, 1}, PutOptions{Expect: Version(5), TTLMs: 60_000, Local: true}); err != nil {
		t.Fatal(err)
	}
	if got := (*calls)[1].headers; !reflect.DeepEqual(got, [][2]string{
		{"x-wasmkeep-kv-expect", "5"}, {"x-wasmkeep-kv-durable", "0"}, {"x-wasmkeep-kv-ttl-ms", "60000"},
	}) {
		t.Fatal(got)
	}
	if !reflect.DeepEqual((*calls)[1].body, []byte{0, 1}) {
		t.Fatal((*calls)[1].body)
	}
	store.CompareAndSet("user:42", Version(6), []byte("x"))
	store.CompareAndSet("user:42", Absent(), []byte("x"))
	if (*calls)[2].headers[0][1] != "6" || (*calls)[3].headers[0][1] != "absent" {
		t.Fatal((*calls)[2].headers, (*calls)[3].headers)
	}
	// A nil value is sent as an empty body.
	store.Put("k", nil)
	if b := (*calls)[4].body; b == nil || len(b) != 0 {
		t.Fatal("nil value")
	}
}

func TestKvAnswersConflictRefusalMalformedFailedCallLongKey(t *testing.T) {
	answerWith := answer{}
	c, calls := fake(func(request) answer { return answerWith })
	store := New("kv", 4, c)

	answerWith = status(409, "conflict\n")
	_, err := store.Put("k", []byte("v"))
	expectErr(t, err, ErrConflict, "conflict: the key changed", 0)
	answerWith = status(500, "storage: disk full\n")
	_, err = store.Get("k")
	expectErr(t, err, ErrRefused, "shard answered 500: storage: disk full", 500)
	answerWith = ok(make([]byte, 3))
	_, err = store.Get("k")
	expectErr(t, err, ErrRefused, "shard answered 200: malformed reply from shard", 200)
	answerWith = ok([]byte{})
	_, err = store.Put("k", []byte("v"))
	expectErr(t, err, ErrRefused, "", 200)
	answerWith = ok([]byte{})
	_, err = store.Delete("k")
	expectErr(t, err, ErrRefused, "", 200)
	if errors.Is(err, ErrConflict) || errors.Is(err, ErrCall) {
		t.Fatal("errors.Is matches only its own kind")
	}

	address := "kv/shard-" + string(rune('0'+ShardFor("k", 4)))
	for _, f := range []struct{ kind, detail, want string }{
		{"timeout", "", "call to " + address + " timed out"},
		{"failed", "trapped", "call to " + address + " failed: trapped"},
		{"reentrant", "", "call to " + address + " would re-enter an actor already in this call chain"},
		{"invalid-address", "no class kv", "invalid address " + address + ": no class kv"},
		{"too-large", "", "call to " + address + ": body too large"},
	} {
		answerWith = answer{err: call.Failure(address, f.kind, f.detail)}
		_, err = store.Get("k")
		expectErr(t, err, ErrCall, f.want, 0)
		_, err = store.Put("k", []byte("v"))
		expectErr(t, err, ErrCall, f.want, 0)
		_, err = store.Delete("k")
		expectErr(t, err, ErrCall, f.want, 0)
	}

	answerWith = ok(u64(1))
	before := len(*calls)
	_, err = store.Put(strings.Repeat("k", 1025), []byte("v"))
	expectErr(t, err, ErrKeyTooLong, "key longer than 1024 bytes", 0)
	if v, err := store.Put(strings.Repeat("k", 1024), []byte("v")); err != nil || v != 1 {
		t.Fatal("1024 bytes is allowed", err)
	}
	// The limit is on bytes: 513 two-byte characters are too long.
	_, err = store.Get(strings.Repeat("é", 513))
	expectErr(t, err, ErrKeyTooLong, "", 0)
	_, err = store.DeleteIf(strings.Repeat("k", 1025), 1)
	expectErr(t, err, ErrKeyTooLong, "", 0)
	if len(*calls) != before+1 {
		t.Fatal("a long key is refused before any call")
	}
}

func TestGetDecodesAnEntryAnd404IsAbsent(t *testing.T) {
	answerWith := ok(encodeEntry(&Entry{Value: []byte("Ada"), Version: 3, ExpiresAt: 99}))
	c, calls := fake(func(request) answer { return answerWith })
	store := New("kv", 1, c)
	e, err := store.Get("user:1")
	if err != nil || !reflect.DeepEqual(e, &Entry{Value: []byte("Ada"), Version: 3, ExpiresAt: 99}) {
		t.Fatal(e, err)
	}
	want := request{"kv/shard-0", "GET", "/wasmkeep-kv/v1/k/" + wire.Hex([]byte("user:1")), [][2]string{}, []byte{}}
	if !reflect.DeepEqual((*calls)[0], want) {
		t.Fatalf("%+v", (*calls)[0])
	}
	answerWith = status(404, "no such key\n")
	if e, err := store.GetFresh("user:1"); err != nil || e != nil {
		t.Fatal(e, err)
	}
}

func TestDeleteAndDeleteIf(t *testing.T) {
	answerWith := ok(u64(1))
	c, calls := fake(func(request) answer { return answerWith })
	store := New("kv", 1, c)
	if removed, err := store.Delete("k"); err != nil || !removed {
		t.Fatal(removed, err)
	}
	want := request{"kv/shard-0", "DELETE", "/wasmkeep-kv/v1/k/6b", [][2]string{{"x-wasmkeep-kv-expect", "any"}}, []byte{}}
	if !reflect.DeepEqual((*calls)[0], want) {
		t.Fatalf("%+v", (*calls)[0])
	}
	answerWith = ok(u64(0))
	if removed, err := store.Delete("k"); err != nil || removed {
		t.Fatal(removed, err)
	}
	answerWith = status(409, "")
	_, err := store.DeleteIf("k", 4)
	expectErr(t, err, ErrConflict, "", 0)
	if got := (*calls)[2].headers; !reflect.DeepEqual(got, [][2]string{{"x-wasmkeep-kv-expect", "4"}}) {
		t.Fatal(got)
	}
}

func TestListAsksEveryShardAndMergesThePages(t *testing.T) {
	pages := map[string][]string{
		"kv/shard-0": {"user:3"},
		"kv/shard-1": {"user:1", "user:4"},
		"kv/shard-2": {"user:2"},
	}
	refuse := false
	c, calls := fake(func(r request) answer {
		if refuse {
			return status(409, "conflict")
		}
		return ok(encodeKeys(pages[r.address]))
	})
	store := New("kv", 3, c)
	keys, err := store.ListAfter("user:", "user:0", 3)
	if err != nil || !reflect.DeepEqual(keys, []string{"user:1", "user:2", "user:3"}) {
		t.Fatal(keys, err)
	}
	for i, r := range *calls {
		want := request{
			"kv/shard-" + string(rune('0'+i)), "GET",
			"/wasmkeep-kv/v1/list?prefix=" + wire.Hex([]byte("user:")) + "&limit=3&after=" + wire.Hex([]byte("user:0")),
			[][2]string{}, []byte{},
		}
		if !reflect.DeepEqual(r, want) {
			t.Fatalf("%+v", r)
		}
	}
	keys, err = store.List("", 10)
	if err != nil || len(keys) != 4 {
		t.Fatal(keys, err)
	}
	if p := (*calls)[3].path; p != "/wasmkeep-kv/v1/list?prefix=&limit=10" {
		t.Fatal(p)
	}
	// An empty after is not no after: it skips the empty key.
	store.ListAfter("", "", 10)
	if p := (*calls)[6].path; p != "/wasmkeep-kv/v1/list?prefix=&limit=10&after=" {
		t.Fatal(p)
	}
	refuse = true
	_, err = store.List("", 10)
	expectErr(t, err, ErrRefused, "", 409)
}

func TestReadCacheServesGetAndFollowsThisClientsWrites(t *testing.T) {
	clock := uint64(1_000_000)
	saved := nowMs
	nowMs = func() uint64 { return clock }
	defer func() { nowMs = saved }()

	value, version := "one", uint64(1)
	refuse := false
	c, calls := fake(func(r request) answer {
		switch {
		case refuse:
			return status(409, "")
		case r.method == "GET":
			return ok(encodeEntry(&Entry{Value: []byte(value), Version: version}))
		case r.method == "PUT":
			version++
			return ok(u64(version))
		}
		return ok(u64(1))
	})
	// A class name of its own: the cache is per instance, shared by clients.
	cached := New("cache-test", 2, c).WithReadCache(60_000)
	fresh := New("cache-test", 2, c)
	get := func(cl *Client, key string) string {
		t.Helper()
		e, err := cl.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if e == nil {
			return "<absent>"
		}
		return string(e.Value)
	}
	if get(cached, "flag") != "one" {
		t.Fatal("first read")
	}
	value = "two" // another actor's write
	if get(cached, "flag") != "one" {
		t.Fatal("served from the cache")
	}
	if get(fresh, "flag") != "two" {
		t.Fatal("a client without a cache asks")
	}
	if e, _ := cached.GetFresh("flag"); string(e.Value) != "two" {
		t.Fatal("get fresh")
	}
	value = "three"
	if get(cached, "flag") != "two" {
		t.Fatal("GetFresh refreshed the cache")
	}
	if len(*calls) != 3 {
		t.Fatal(len(*calls))
	}
	clock += 60_000
	if get(cached, "flag") != "three" {
		t.Fatal("an entry as old as the max age is a miss")
	}

	// Its own writes go through the cache: a put, then a delete.
	mine := []byte("mine")
	cached.Put("flag", mine)
	mine[0] = 'X' // the cache kept a copy
	if e, _ := cached.Get("flag"); !reflect.DeepEqual(e, &Entry{Value: []byte("mine"), Version: version}) {
		t.Fatal(e)
	}
	cached.Delete("flag")
	if get(cached, "flag") != "<absent>" {
		t.Fatal("a delete caches the absence")
	}
	// A conflict forgets the key: the next get asks the shard.
	refuse = true
	_, err := cached.CompareAndSet("flag", Version(1), []byte("x"))
	expectErr(t, err, ErrConflict, "", 0)
	refuse = false
	value = "theirs"
	before := len(*calls)
	if get(cached, "flag") != "theirs" || len(*calls) != before+1 {
		t.Fatal("asked after a conflict")
	}
	// A failed call forgets it too.
	failing := New("cache-test", 2, func(string, string, string, [][2]string, []byte) (uint16, []byte, error) {
		return 0, nil, call.Failure("x", "timeout", "")
	}).WithReadCache(60_000)
	if _, err := failing.Put("flag", []byte("lost?")); !errors.Is(err, ErrCall) {
		t.Fatal(err)
	}
	before = len(*calls)
	get(cached, "flag")
	if len(*calls) != before+1 {
		t.Fatal("a failed write forgets the key")
	}
	// A ttl write caches its expiry, from when it was sent.
	cached.PutWith("session", []byte("s"), PutOptions{TTLMs: 5_000})
	if e, _ := cached.Get("session"); e == nil || e.ExpiresAt != clock+5_000 {
		t.Fatal(e)
	}
	clock += 5_000
	if get(cached, "session") != "<absent>" {
		t.Fatal("expired in the cache")
	}
}
