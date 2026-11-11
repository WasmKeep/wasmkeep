package wasmkeep

// Host tests of what runs without the host: value conversion, rows,
// requests, responses and URL splitting. The host calls themselves are
// covered by the black-box tests in crates/wasmkeep-server/tests
// (languages, chat, jobs_langs, workflows_langs, fetch_langs).

import (
	"bytes"
	"errors"
	"testing"

	"github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasmkeep/actor/types"
)

func TestParams(t *testing.T) {
	p, err := toValues([]any{nil, true, false, 7, int8(-3), uint32(9), uint64(1 << 62), 1.5, float32(2), "hi", []byte{1, 2}})
	if err != nil {
		t.Fatal(err)
	}
	got := p.Slice()
	if !got[0].Null() {
		t.Error("nil is NULL")
	}
	ints := map[int]int64{1: 1, 2: 0, 3: 7, 4: -3, 5: 9, 6: 1 << 62}
	for i, want := range ints {
		if v := got[i].Integer(); v == nil || *v != want {
			t.Errorf("param %d: got %v, want %d", i, got[i], want)
		}
	}
	if v := got[7].Real(); v == nil || *v != 1.5 {
		t.Error("float64")
	}
	if v := got[8].Real(); v == nil || *v != 2 {
		t.Error("float32")
	}
	if v := got[9].Text(); v == nil || *v != "hi" {
		t.Error("string")
	}
	if v := got[10].Blob(); v == nil || !bytes.Equal(v.Slice(), []byte{1, 2}) {
		t.Error("blob")
	}
}

func TestParamsRefused(t *testing.T) {
	for _, p := range []any{uint64(1 << 63), struct{}{}, []int{1}} {
		_, err := toValues([]any{"ok", p})
		var e *SQLError
		if !errors.As(err, &e) || e.Kind != "bind" || e.Message != "parameter 2: unsupported type" {
			t.Errorf("%T: got %v", p, err)
		}
	}
}

func TestRows(t *testing.T) {
	r := fromRows(&types.Rows{})
	if r.Len() != 0 || r.Get(0, 0) != nil || r.Int(0, 0) != 0 {
		t.Error("empty rows")
	}
	r = &Rows{
		Columns: []string{"n", "f", "s", "b", "z"},
		Values:  [][]any{{int64(42), 2.5, "17", []byte("raw"), nil}},
	}
	if r.Column("s") != 2 || r.Column("missing") != -1 {
		t.Error("Column")
	}
	if r.Int(0, 0) != 42 || r.Int(0, 1) != 2 || r.Int(0, 2) != 0 || r.Int(0, 3) != 0 {
		t.Error("Int")
	}
	if r.Float(0, 0) != 42 || r.Float(0, 1) != 2.5 || r.Float(0, 2) != 0 {
		t.Error("Float")
	}
	if r.String(0, 0) != "42" || r.String(0, 1) != "2.5" || r.String(0, 3) != "raw" || r.String(0, 4) != "" {
		t.Error("String")
	}
	if string(r.Bytes(0, 2)) != "17" || string(r.Bytes(0, 3)) != "raw" || r.Bytes(0, 0) != nil {
		t.Error("Bytes")
	}
	if !r.IsNull(0, 4) || r.IsNull(0, 0) || !r.IsNull(1, 0) || !r.IsNull(0, 9) {
		t.Error("IsNull")
	}
}

func TestValuesBack(t *testing.T) {
	cases := []struct {
		v    types.Value
		want any
	}{
		{types.ValueNull(), nil},
		{types.ValueInteger(-5), int64(-5)},
		{types.ValueReal(0.25), 0.25},
		{types.ValueText("t"), "t"},
	}
	for _, c := range cases {
		if got := fromValue(c.v); got != c.want {
			t.Errorf("got %#v, want %#v", got, c.want)
		}
	}
}

func TestRequest(t *testing.T) {
	r := NewRequest("POST", "/a?b=1", [][2]string{{"content-type", "text/plain"}, {"x-two", "1"}, {"x-two", "2"}}, []byte("body"))
	if r.Header("Content-Type") != "text/plain" || r.Header("x-two") != "1" || r.Header("nope") != "" {
		t.Error("Header")
	}
	if _, ok := r.LookupHeader("NOPE"); ok {
		t.Error("LookupHeader")
	}
	if r.Text() != "body" {
		t.Error("Text")
	}
	if b, err := r.ReadBody(); err != nil || string(b) != "body" {
		t.Error("ReadBody")
	}
}

func TestResponses(t *testing.T) {
	r := Text(201, "made\n").WithHeader("etag", "3")
	if r.Status != 201 || string(r.Body) != "made\n" || len(r.Headers) != 1 || r.Headers[0] != [2]string{"etag", "3"} {
		t.Errorf("Text: %+v", r)
	}
	j := JSON(200, `{"a":1}`)
	if j.Headers[0] != [2]string{"content-type", "application/json"} {
		t.Errorf("JSON: %+v", j)
	}
	b := Bytes(200, "application/octet-stream", []byte{0})
	if b.Headers[0][1] != "application/octet-stream" || len(b.Body) != 1 {
		t.Errorf("Bytes: %+v", b)
	}
}

func TestParseURL(t *testing.T) {
	ok := []struct{ url, authority, path string }{
		{"http://example.com", "example.com", "/"},
		{"https://example.com:8443/a/b?c=d", "example.com:8443", "/a/b?c=d"},
		{"http://h?q=1", "h", "/?q=1"},
	}
	for _, c := range ok {
		got, err := parseURL(c.url)
		if err != nil || got.authority != c.authority || got.path != c.path || got.https != (c.url[4] == 's') {
			t.Errorf("%s: got %+v, %v", c.url, got, err)
		}
	}
	bad := map[string]string{
		"example.com":         "example.com: no scheme",
		"ftp://example.com/":  "ftp://example.com/: unsupported scheme ftp",
		"http:///nothing":     "http:///nothing: no host",
		"https://?only=query": "https://?only=query: no host",
	}
	for url, want := range bad {
		if _, err := parseURL(url); err == nil || err.Error() != want {
			t.Errorf("%s: got %v, want %q", url, err, want)
		}
	}
}

func TestEqualFold(t *testing.T) {
	if !equalFold("Content-Type", "content-type") || equalFold("a", "ab") || equalFold("a-b", "a_b") {
		t.Error("equalFold")
	}
}
