// A test guest for crates/wasmkeep-server/tests/fetch_langs.rs: the Go
// SDK's Fetch behind the same route as the Rust test-actor's.
//
// GET /fetch?url=U&send=N fetches U (percent-encoded) with a 5 s timeout
// and a 4096-byte limit: a GET, or a POST of N bytes of `x` when N > 0.
// It answers `STATUS:BODY`, or `error: ...`.
//
// GET /fetch-all?u=I&u=... (each I percent-encoded `N URL`) sends those
// requests, GETs or POSTs of N bytes, with FetchAll, and answers one line
// each, as /fetch words it; with seq=1, one Fetch after the other instead.
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
	if req.Method == "GET" && path == "/fetch-all" {
		return fetchAll(query)
	}
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

func fetchAll(query string) wasmkeep.Response {
	q, err := url.ParseQuery(query)
	if err != nil {
		return wasmkeep.Text(400, err.Error()+"\n")
	}
	var batch []wasmkeep.FetchRequest
	for _, item := range q["u"] {
		n, u, _ := strings.Cut(item, " ")
		send, _ := strconv.Atoi(n)
		r := wasmkeep.FetchRequest{URL: u, Options: wasmkeep.FetchOptions{Timeout: 5 * time.Second, Limit: 4096}}
		if send > 0 {
			r.Options.Method = "POST"
			r.Options.Body = []byte(strings.Repeat("x", send))
		}
		batch = append(batch, r)
	}
	var results []wasmkeep.FetchResult
	switch {
	case q.Get("seq") != "":
		for _, r := range batch {
			f, err := wasmkeep.Fetch(r.URL, r.Options)
			results = append(results, wasmkeep.FetchResult{Fetched: f, Err: err})
		}
	default:
		results = wasmkeep.FetchAll(batch)
	}
	var out strings.Builder
	for _, r := range results {
		if r.Err != nil {
			out.WriteString("error: " + r.Err.Error() + "\n")
		} else {
			out.WriteString(strconv.Itoa(int(r.Fetched.Status)) + ":" + string(r.Fetched.Body) + "\n")
		}
	}
	return wasmkeep.Text(200, out.String())
}

func main() {}
