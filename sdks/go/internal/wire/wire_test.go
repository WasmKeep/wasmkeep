package wire

import (
	"bytes"
	"encoding/hex"
	"testing"
)

func TestFramingRoundTripsAndRejectsTruncation(t *testing.T) {
	var w Writer
	buf := w.U64(7).Bytes([]byte("abc")).Bytes([]byte{}).U64(^uint64(0)).Finish()
	r := NewReader(buf)
	if v, ok := r.U64(); !ok || v != 7 {
		t.Fatalf("u64 = %d %v", v, ok)
	}
	if v, ok := r.Bytes(); !ok || string(v) != "abc" {
		t.Fatalf("bytes = %q %v", v, ok)
	}
	if v, ok := r.Bytes(); !ok || len(v) != 0 {
		t.Fatalf("empty bytes = %q %v", v, ok)
	}
	if v, ok := r.U64(); !ok || v != ^uint64(0) {
		t.Fatalf("max = %d %v", v, ok)
	}
	if !r.IsEmpty() {
		t.Fatal("not empty")
	}
	if _, ok := r.U64(); ok {
		t.Fatal("read past the end")
	}
	// A length that runs past the end is refused, not truncated.
	if _, ok := NewReader(buf[8:12]).Bytes(); ok {
		t.Fatal("truncated bytes accepted")
	}
	// Bytes are copied out: the buffer may be reused.
	r = NewReader(buf[8:])
	v, _ := r.Bytes()
	buf[12] = 'X'
	if string(v) != "abc" {
		t.Fatal("bytes alias the buffer")
	}
}

func TestFramingIsBigEndianAsRustWritesIt(t *testing.T) {
	var w Writer
	buf := w.U64(7).Bytes([]byte("abc")).U64(0x0102030405060708).Finish()
	want := "0000000000000007" + "00000003616263" + "0102030405060708"
	if got := hex.EncodeToString(buf); got != want {
		t.Fatalf("got %s, want %s", got, want)
	}
	var empty Writer
	if b := empty.Finish(); b == nil || len(b) != 0 {
		t.Fatal("an empty writer finishes as an empty, non-nil slice")
	}
}

func TestHexIsLowercaseTwoDigitsPerByte(t *testing.T) {
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	h := Hex(all)
	if len(h) != 512 {
		t.Fatalf("len %d", len(h))
	}
	back, err := hex.DecodeString(h)
	if err != nil || !bytes.Equal(back, all) {
		t.Fatal("does not round-trip")
	}
	if got := Hex([]byte{0, 255, 0x41}); got != "00ff41" {
		t.Fatalf("got %s", got)
	}
	if Hex(nil) != "" {
		t.Fatal("empty")
	}
}

func TestLossyReplacesInvalidBytesAndTrims(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"storage: disk full\n", "storage: disk full"},
		{"  clé é \r\n", "clé é"},
		{"a\xffb", "a�b"},
		{"", ""},
	} {
		if got := Lossy([]byte(c.in)); got != c.want {
			t.Errorf("Lossy(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
