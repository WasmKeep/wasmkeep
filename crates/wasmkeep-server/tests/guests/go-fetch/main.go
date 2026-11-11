// A test guest for crates/wasmkeep-server/tests/fetch_langs.rs: the Go
// SDK's Fetch behind the same route as the Rust test-actor's.
//
// GET /fetch?url=U&send=N fetches U (percent-encoded) with a 5 s timeout
// and a 4096-byte limit: a GET, or a POST of N bytes of `x` when N > 0.
// It answers `STATUS:BODY`, or `error: ...`.
package main

import (
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/wasmkeep/wasmkeep/sdks/go/wasmkeep"
)

func init() {
	wasmkeep.Serve(wasmkeep.Actor{Fetch: fetch})
}

func fetch(req *wasmkeep.Request) wasmkeep.Response {
	path, query, _ := strings.Cut(req.Path, "?")
	if req.Method != "GET" || path != "/fetch" {
		return wasmkeep.Text(404, "GET /fetch?url=...\n")
	}
	q, err := url.ParseQuery(query)
	if err != nil {
		return wasmkeep.Text(400, err.Error()+"\n")
	}
	send, _ := strconv.Atoi(q.Get("send"))
	opts := wasmkeep.FetchOptions{Timeout: 5 * time.Second, Limit: 4096}
	if send > 0 {
		opts.Method = "POST"
		opts.Body = []byte(strings.Repeat("x", send))
	}
	r, err := wasmkeep.Fetch(q.Get("url"), opts)
	if err != nil {
		return wasmkeep.Text(200, "error: "+err.Error())
	}
	return wasmkeep.Text(200, strconv.Itoa(int(r.Status))+":"+string(r.Body))
}

func main() {}
