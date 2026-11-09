package kv

import (
	"encoding/hex"
	"reflect"
	"strconv"
	"testing"

	"github.com/wasmkeep/wasmkeep/sdks/go/internal/wire"
)

func TestShardHashIsFNV1a64AndMatchesRust(t *testing.T) {
	// Known FNV-1a 64 values: "" and "a" (kv.rs).
	if got, want := ShardFor("", 0xffff_ffff), uint32(0xcbf2_9ce4_8422_2325%0xffff_ffff); got != want {
		t.Fatalf("empty: %d, want %d", got, want)
	}
	if got, want := ShardFor("a", 1<<16), uint32(0xaf63_dc4c_8601_ec8c%(1<<16)); got != want {
		t.Fatalf("a: %d, want %d", got, want)
	}
	if ShardFor("anything", 1) != 0 || ShardFor("anything", 0) != 0 {
		t.Fatal("one shard, or zero meaning one")
	}
	// Computed with the Rust `shard_for` (the TypeScript SDK's vectors):
	// key, n=4, n=8, n=1000, n=u32::MAX.
	vectors := []struct {
		key  string
		want [4]uint32
	}{
		{"", [4]uint32{1, 5, 37, 1343537162}},
		{"a", [4]uint32{0, 4, 996, 895863001}},
		{"user:42", [4]uint32{2, 2, 410, 1223114855}},
		{"user:0", [4]uint32{0, 0, 736, 1829090531}},
		{"user:1", [4]uint32{3, 3, 947, 1829091222}},
		{"user:2", [4]uint32{2, 6, 158, 1829091913}},
		{"job:1", [4]uint32{1, 1, 481, 310798266}},
		{"greeting", [4]uint32{2, 6, 198, 2089897798}},
		{"clé é", [4]uint32{2, 6, 550, 819157025}},
		{"\x00\xff\x80", [4]uint32{0, 0, 392, 1085081017}},
	}
	for _, v := range vectors {
		got := [4]uint32{ShardFor(v.key, 4), ShardFor(v.key, 8), ShardFor(v.key, 1000), ShardFor(v.key, 0xffff_ffff)}
		if got != v.want {
			t.Errorf("key %q: %v, want %v", v.key, got, v.want)
		}
	}
	// Spread: 1,000 keys over 8 shards, the exact counts Rust gets.
	var seen [8]int
	for i := 0; i < 1000; i++ {
		seen[ShardFor("user:"+strconv.Itoa(i), 8)]++
	}
	if want := [8]int{126, 125, 125, 126, 124, 125, 125, 124}; seen != want {
		t.Fatalf("spread %v, want %v", seen, want)
	}
	if ShardName(3) != "shard-3" {
		t.Fatal(ShardName(3))
	}
}

func TestExpectations(t *testing.T) {
	if !Any().Allows(0, false) || !Any().Allows(3, true) {
		t.Fatal("any")
	}
	if !Absent().Allows(0, false) || Absent().Allows(3, true) {
		t.Fatal("absent")
	}
	if !Version(3).Allows(3, true) || Version(3).Allows(4, true) || Version(3).Allows(3, false) {
		t.Fatal("version")
	}
	if (Expect{}) != Any() {
		t.Fatal("the zero value is any")
	}
	for e, want := range map[Expect]string{Any(): "any", Absent(): "absent", Version(42): "42", Version(0): "0", Version(^uint64(0)): "18446744073709551615"} {
		if got := e.encode(); got != want {
			t.Errorf("encode %v = %q, want %q", e, got, want)
		}
	}
}

func TestEntriesAndKeyListsRoundTrip(t *testing.T) {
	e := &Entry{Value: []byte("v"), Version: 9, ExpiresAt: 1234}
	got, ok := decodeEntry(encodeEntry(e))
	if !ok || !reflect.DeepEqual(got, e) {
		t.Fatalf("%v %v", got, ok)
	}
	if h := hex.EncodeToString(encodeEntry(e)); h != "0000000000000009"+"00000000000004d2"+"0000000176" {
		t.Fatal(h)
	}
	forever := &Entry{Value: []byte("v"), Version: 9}
	if got, ok := decodeEntry(encodeEntry(forever)); !ok || !reflect.DeepEqual(got, forever) {
		t.Fatal("forever")
	}
	// Trailing bytes are refused.
	if _, ok := decodeEntry(append(encodeEntry(e), 0)); ok {
		t.Fatal("trailing byte accepted")
	}
	// A version past 2^53 is fine in Go (the TypeScript SDK refuses it).
	var w wire.Writer
	big, ok := decodeEntry(w.U64(1 << 60).U64(0).Bytes([]byte("v")).Finish())
	if !ok || big.Version != 1<<60 {
		t.Fatal("big version")
	}
	keys := []string{"a", "", "\x00\xff"}
	if got, ok := decodeKeys(encodeKeys(keys)); !ok || !reflect.DeepEqual(got, keys) {
		t.Fatalf("keys %q", got)
	}
	if _, ok := decodeKeys([]byte{0, 0, 0, 0, 0, 0, 0, 1}); ok {
		t.Fatal("a count past the end accepted")
	}
}

func TestPagesMergeInByteOrder(t *testing.T) {
	pages := [][]string{{"a", "d"}, {"b", "c", "e"}, {}}
	if got := mergePages(pages, 4); !reflect.DeepEqual(got, []string{"a", "b", "c", "d"}) {
		t.Fatal(got)
	}
	// Byte order: "é" (c3 a9) sorts after "z" (7a), a prefix before its
	// extensions.
	if got := mergePages([][]string{{"é"}, {"z", "za"}, {""}}, 10); !reflect.DeepEqual(got, []string{"", "z", "za", "é"}) {
		t.Fatal(got)
	}
	if got := mergePages(nil, 3); got == nil || len(got) != 0 {
		t.Fatal("an empty listing is an empty slice")
	}
}

func TestReadCacheAgesExpiresAndStaysBounded(t *testing.T) {
	c := newReadCache(2)
	e := func(v, exp uint64) *Entry { return &Entry{Value: []byte{}, Version: v, ExpiresAt: exp} }
	check := func(key string, now uint64, wantHit bool, want *Entry) {
		t.Helper()
		got, hit := c.get(key, now, 100)
		if hit != wantHit || !reflect.DeepEqual(got, want) {
			t.Fatalf("get(%q, %d) = %v %v, want %v %v", key, now, got, hit, want, wantHit)
		}
	}
	c.insert("k", 100, e(1, 0))
	check("k", 150, true, e(1, 0))
	check("k", 200, false, nil)   // too old
	check("k", 50, true, e(1, 0)) // a clock that went back: age 0
	c.insert("gone", 100, nil)
	check("gone", 120, true, nil)   // absence is cached
	check("other", 120, false, nil) // a miss is not an absence

	// An entry that expired while cached reads as absent.
	c.insert("k", 100, e(2, 130))
	check("k", 129, true, e(2, 130))
	check("k", 130, true, nil)

	// Full: expired entries are dropped first, then everything.
	c.insert("third", 140, e(3, 0))
	if len(c.entries) > 2 {
		t.Fatalf("%d entries", len(c.entries))
	}
	check("third", 141, true, e(3, 0))
	c.remove("third")
	check("third", 141, false, nil)

	// The cache keeps copies: neither the inserted nor the returned value
	// aliases it.
	v := []byte("abc")
	c.insert("copy", 100, &Entry{Value: v, Version: 1})
	v[0] = 'X'
	got, _ := c.get("copy", 101, 100)
	got.Value[1] = 'Y'
	again, _ := c.get("copy", 101, 100)
	if string(again.Value) != "abc" {
		t.Fatalf("aliased: %q", again.Value)
	}
}
