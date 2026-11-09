// The app class of the jobs example, in Go (TinyGo): an HTTP API over the
// Go SDK's kv and queue clients (sdks/go), and the consumer the `jobs`
// queue pushes to. It talks to the Rust shard and queue classes of
// examples/jobs (`kv` and `queue`) and must be deployed as class `app`:
// `queue/jobs` delivers to `app/worker`. The routes and bodies are those
// of examples/ts-jobs.
//
// Key-value (keys spread over 4 shards of class `kv`):
//
//   - `GET /kv/<key>`: the value, with its version in `etag`; 404 if absent.
//     `?cached=1` reads through this actor's 60 s read cache.
//   - `PUT /kv/<key>`: stores the body and answers the new version.
//     `if-match: <version>` or `if-none-match: *` make it a compare-and-set
//     (409 when it does not hold); `?ttl_ms=N` expires it.
//   - `DELETE /kv/<key>`: 200 if it was there, 404 otherwise; with
//     `if-match: <version>`, only if it holds that version (409 otherwise).
//   - `GET /kv?prefix=P`: the keys starting with `P`, one per line.
//
// Queues (actors of class `queue`):
//
//   - `POST /jobs`: queues the body on `queue/jobs`, which pushes it to
//     `app/worker`. The worker stores the body in upper case at key
//     `job:<id>`; a body starting with `fail` asks for a retry every time,
//     so it ends in the dead-letter table after three attempts.
//   - `GET /jobs/stats`, `GET /jobs/dead`: counts and dead letters.
//   - `POST /inbox`: queues the body on `queue/inbox`, a pull queue.
//   - `POST /inbox/batch`: queues each line of the body, in one write;
//     answers their ids, one per line.
//   - `GET /inbox/stats`: the inbox's counts.
//   - `POST /inbox/lease?visibility_ms=N&max=M`: leases up to `M` (default
//     one) messages, answered as `<id> <attempt> <body>` lines.
//   - `POST /inbox/ack?id=I&attempt=A`: 200 if that lease was current, 409
//     if it ran out (the message was leased again) or was settled.
//
// Workflows (instances of the Rust `orders` class of examples/workflows,
// through the Go SDK's workflow client; the routes and bodies of
// examples/workflows/app):
//
//   - `POST /orders/<id>`: starts order `<id>` with the body as its input;
//     answers its status. Starting an id again answers the existing
//     order's status.
//   - `GET /orders/<id>`: its status, one `key: value` per line; 404 if
//     there is none.
//   - `POST /orders/<id>/approve`: delivers the `approved` event; answers
//     the status.
//   - `POST /orders/<id>/cancel`: cancels it; answers the status.
package main

import (
	"errors"
	"strconv"
	"strings"

	"github.com/wasmkeep/wasmkeep/sdks/go/call"
	"github.com/wasmkeep/wasmkeep/sdks/go/kv"
	"github.com/wasmkeep/wasmkeep/sdks/go/queue"
	"github.com/wasmkeep/wasmkeep/sdks/go/workflow"
	"go.bytecodealliance.org/cm"
	incominghandler "wasmkeep.example/go-jobs/gen/wasi/http/incoming-handler"
	httptypes "wasmkeep.example/go-jobs/gen/wasi/http/types"
	"wasmkeep.example/go-jobs/gen/wasmkeep/actor/actor"
	actorcore "wasmkeep.example/go-jobs/gen/wasmkeep/actor/actor-core"
	"wasmkeep.example/go-jobs/gen/wasmkeep/actor/actors"
)

// callActor adapts this app's generated actors.Call binding to the SDK's
// call.Caller.
func callActor(address, method, path string, headers [][2]string, body []byte) (uint16, []byte, error) {
	r := actors.Call(address, actors.Request{
		Method:  method,
		Path:    path,
		Headers: cm.ToList(headers),
		Body:    cm.ToList(body),
	})
	if e := r.Err(); e != nil {
		detail := ""
		if s := e.InvalidAddress(); s != nil {
			detail = *s
		} else if s := e.Failed(); s != nil {
			detail = *s
		}
		return 0, nil, call.Failure(address, e.String(), detail)
	}
	return r.OK().Status, r.OK().Body.Slice(), nil
}

const shards = 4

// Keys over 4 shards, with a 60 s read cache: Get may use it, writes
// through this client update it, GetFresh skips it.
var (
	store  = kv.New("kv", shards, callActor).WithReadCache(60_000)
	jobs   = queue.New("queue/jobs", callActor)
	inbox  = queue.New("queue/inbox", callActor)
	orders = workflow.New("orders", callActor)
)

type hookResult = cm.Result[string, struct{}, string]

func init() {
	actor.Exports.OnInit = func() hookResult { return cm.OK[hookResult](struct{}{}) }
	actor.Exports.OnAlarm = func() hookResult { return cm.OK[hookResult](struct{}{}) }
	actor.Exports.OnWebsocketOpen = func(uint64, string) hookResult {
		return cm.Err[hookResult]("no WebSockets here")
	}
	actor.Exports.OnWebsocketMessage = func(uint64, actorcore.Message) {}
	actor.Exports.OnWebsocketClose = func(uint64, uint16, string) {}
	incominghandler.Exports.Handle = handle
}

// request is an incoming request, read whole.
type request struct {
	method  string // upper case
	path    string // with the query
	headers [][2]string
	body    []byte
}

func (r *request) header(name string) (string, bool) {
	for _, h := range r.headers {
		if strings.EqualFold(h[0], name) {
			return h[1], true
		}
	}
	return "", false
}

// response is what a route answers.
type response struct {
	status      uint16
	contentType string
	etag        string
	body        []byte
}

func text(status uint16, body string) response {
	return response{status: status, contentType: "text/plain", body: []byte(body)}
}

func param(path, name string) (string, bool) {
	_, query, found := strings.Cut(path, "?")
	if !found {
		return "", false
	}
	for _, pair := range strings.Split(query, "&") {
		k, v, _ := strings.Cut(pair, "=")
		if k == name {
			v, _, _ = strings.Cut(v, "=")
			return v, true
		}
	}
	return "", false
}

// number parses a non-negative integer parameter.
func number(value string, present bool) (uint64, bool) {
	if !present || value == "" {
		return 0, false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseUint(value, 10, 64)
	return n, err == nil
}

func u(v uint64) string { return strconv.FormatUint(v, 10) }

// failure maps the kv and queue errors to a status.
func failure(err error) response {
	if errors.Is(err, kv.ErrConflict) {
		return text(409, "the key changed\n")
	}
	return text(502, err.Error()+"\n")
}

func kvRoute(req *request, key string) response {
	match, hasMatch := req.header("if-match")
	version, hasVersion := number(strings.ReplaceAll(match, `"`, ""), hasMatch)
	if hasMatch && !hasVersion {
		return text(400, "if-match is not a version\n")
	}
	switch req.method {
	case "GET":
		var entry *kv.Entry
		var err error
		if c, _ := param(req.path, "cached"); c == "1" {
			entry, err = store.Get(key)
		} else {
			entry, err = store.GetFresh(key)
		}
		if err != nil {
			return failure(err)
		}
		if entry == nil {
			return text(404, "no such key\n")
		}
		return response{status: 200, contentType: "application/octet-stream", etag: u(entry.Version), body: entry.Value}
	case "PUT":
		ttl, hasTTL := number(param(req.path, "ttl_ms"))
		none, _ := req.header("if-none-match")
		absent := none == "*"
		var v uint64
		var err error
		switch {
		case hasTTL:
			expect := kv.Any()
			if hasVersion {
				expect = kv.Version(version)
			} else if absent {
				expect = kv.Absent()
			}
			v, err = store.PutWith(key, req.body, kv.PutOptions{Expect: expect, TTLMs: ttl})
		case hasVersion:
			v, err = store.CompareAndSet(key, kv.Version(version), req.body)
		case absent:
			v, err = store.CompareAndSet(key, kv.Absent(), req.body)
		default:
			v, err = store.Put(key, req.body)
		}
		if err != nil {
			return failure(err)
		}
		r := text(200, u(v)+"\n")
		r.etag = u(v)
		return r
	case "DELETE":
		var removed bool
		var err error
		if hasVersion {
			removed, err = store.DeleteIf(key, version)
		} else {
			removed, err = store.Delete(key)
		}
		if err != nil {
			return failure(err)
		}
		if removed {
			return text(200, "deleted\n")
		}
		return text(404, "no such key\n")
	}
	return text(405, "use GET, PUT or DELETE\n")
}

func list(path string) response {
	prefix, _ := param(path, "prefix")
	keys, err := store.List(prefix, 1000)
	if err != nil {
		return failure(err)
	}
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k + "\n")
	}
	return text(200, b.String())
}

func lease(path string) response {
	max, ok := number(param(path, "max"))
	if !ok {
		max = 1
	}
	visibility, ok := number(param(path, "visibility_ms"))
	if !ok {
		visibility = 30_000
	}
	if max > 0xffff_ffff {
		return text(400, "max is too large\n")
	}
	messages, err := inbox.Lease(uint32(max), visibility)
	if err != nil {
		return failure(err)
	}
	var b strings.Builder
	for _, m := range messages {
		b.WriteString(u(m.ID) + " " + u(uint64(m.Attempt)) + " " + string(m.Body) + "\n")
	}
	return text(200, b.String())
}

func ack(path string) response {
	id, okID := number(param(path, "id"))
	attempt, okAttempt := number(param(path, "attempt"))
	if !okID || !okAttempt || attempt > 0xffff_ffff {
		return text(400, "give id and attempt\n")
	}
	acked, err := inbox.Ack(queue.Message{ID: id, Attempt: uint32(attempt)})
	if err != nil {
		return failure(err)
	}
	if acked {
		return text(200, "acked\n")
	}
	return text(409, "that lease is no longer current\n")
}

func stats(q *queue.Client) response {
	s, err := q.Stats()
	if err != nil {
		return failure(err)
	}
	return text(200, "ready="+u(s.Ready)+" delayed="+u(s.Delayed)+" in_flight="+u(s.InFlight)+" dead="+u(s.Dead)+"\n")
}

func dead(q *queue.Client) response {
	items, err := q.DeadLetters(100)
	if err != nil {
		return failure(err)
	}
	var b strings.Builder
	for _, d := range items {
		b.WriteString(u(d.Message.ID) + " " + u(uint64(d.Message.Attempt)) + " " + d.Reason + "\n")
	}
	return text(200, b.String())
}

func send(q *queue.Client, body []byte) response {
	id, err := q.Send(body)
	if err != nil {
		return failure(err)
	}
	return text(200, u(id)+"\n")
}

func sendBatch(body []byte) response {
	var bodies [][]byte
	for _, line := range strings.Split(string(body), "\n") {
		if line != "" {
			bodies = append(bodies, []byte(line))
		}
	}
	ids, err := inbox.SendBatch(bodies)
	if err != nil {
		return failure(err)
	}
	var b strings.Builder
	for _, id := range ids {
		b.WriteString(u(id) + "\n")
	}
	return text(200, b.String())
}

// work is the worker: one outcome per pushed job. Writing `job:<id>` is
// idempotent, so a duplicate delivery does no harm.
func work(m queue.Message) queue.Outcome {
	body := string(m.Body)
	if strings.HasPrefix(body, "fail") {
		return queue.Retry()
	}
	if _, err := store.Put("job:"+u(m.ID), []byte(strings.ToUpper(body))); err != nil {
		return queue.Retry()
	}
	return queue.Ack()
}

// show is a status as `key: value` lines, as examples/workflows/app
// writes it.
func show(s *workflow.Status) string {
	out := "state: " + string(s.State) + "\npositions: " + u(s.Positions) + "\n"
	if s.WakeAt != 0 {
		out += "wake_at: " + u(s.WakeAt) + "\n"
	}
	if len(s.WaitingFor) > 0 {
		out += "waiting_for: " + strings.Join(s.WaitingFor, ",") + "\n"
	}
	if s.Output != nil {
		out += "output: " + strings.ToValidUTF8(string(s.Output), "\uFFFD") + "\n"
	}
	if s.Error != "" {
		out += "error: " + s.Error + "\n"
	}
	return out
}

func orderReply(s *workflow.Status, err error) response {
	switch {
	case err == nil:
		return text(200, show(s))
	case errors.Is(err, workflow.ErrUnknown):
		return text(404, "no such order\n")
	case errors.Is(err, workflow.ErrFinished):
		return text(409, "the order has finished\n")
	}
	return text(502, err.Error()+"\n")
}

// orderRoute is `/orders/<id>[/approve|/cancel]`; rest is what follows
// `/orders/`.
func orderRoute(req *request, rest string) response {
	id, action, _ := strings.Cut(rest, "/")
	if id == "" {
		return text(404, "not found\n")
	}
	switch {
	case action == "" && req.method == "POST":
		return orderReply(orders.Start(id, req.body))
	case action == "" && req.method == "GET":
		return orderReply(orders.Status(id))
	case action == "approve" && req.method == "POST":
		if err := orders.SendEvent(id, "approved", []byte{}); err != nil {
			return orderReply(nil, err)
		}
		return orderReply(orders.Status(id))
	case action == "cancel" && req.method == "POST":
		return orderReply(orders.Cancel(id))
	}
	return text(404, "not found\n")
}

func route(req *request) response {
	if queue.IsDelivery(req.method, req.path) {
		r := queue.HandleDelivery(queue.CallerFrom(req.headers), req.body, func(_ string, m queue.Message) queue.Outcome {
			return work(m)
		})
		return response{status: r.Status, contentType: r.ContentType, body: r.Body}
	}
	path, _, _ := strings.Cut(req.path, "?")
	get, post := req.method == "GET", req.method == "POST"
	switch {
	case path == "/kv" && get:
		return list(req.path)
	case strings.HasPrefix(path, "/kv/") && len(path) > 4:
		return kvRoute(req, path[4:])
	case path == "/jobs" && post:
		return send(jobs, req.body)
	case path == "/jobs/stats" && get:
		return stats(jobs)
	case path == "/jobs/dead" && get:
		return dead(jobs)
	case path == "/inbox" && post:
		return send(inbox, req.body)
	case path == "/inbox/batch" && post:
		return sendBatch(req.body)
	case path == "/inbox/stats" && get:
		return stats(inbox)
	case path == "/inbox/lease" && post:
		return lease(req.path)
	case path == "/inbox/ack" && post:
		return ack(req.path)
	case strings.HasPrefix(path, "/orders/"):
		return orderRoute(req, strings.Trim(path[len("/orders/"):], "/"))
	}
	return text(404, "see the README for routes\n")
}

// ------------------------------------------------------------ wasi:http

func handle(in httptypes.IncomingRequest, out httptypes.ResponseOutparam) {
	req, ok := read(in)
	in.ResourceDrop()
	if !ok {
		respond(out, text(400, "could not read the request\n"))
		return
	}
	respond(out, route(&req))
}

// read takes the method, path, headers and body of in, dropping every
// child resource it opens (before in itself is dropped).
func read(in httptypes.IncomingRequest) (request, bool) {
	method := in.Method()
	req := request{method: strings.ToUpper(method.String())}
	if other := method.Other(); other != nil {
		req.method = *other
	}
	if p := in.PathWithQuery(); p.Some() != nil {
		req.path = *p.Some()
	}
	headers := in.Headers()
	for _, e := range headers.Entries().Slice() {
		req.headers = append(req.headers, [2]string{string(e.F0), string(cm.List[uint8](e.F1).Slice())})
	}
	headers.ResourceDrop()

	consumed := in.Consume()
	if consumed.IsErr() {
		return req, false
	}
	body := *consumed.OK()
	streamed := body.Stream()
	if streamed.IsErr() {
		body.ResourceDrop()
		return req, false
	}
	stream := *streamed.OK()
	ok := true
	for {
		chunk := stream.BlockingRead(64 * 1024)
		if e := chunk.Err(); e != nil {
			ok = e.Closed()
			break
		}
		req.body = append(req.body, chunk.OK().Slice()...)
	}
	stream.ResourceDrop()
	httptypes.IncomingBodyFinish(body).ResourceDrop()
	if req.body == nil {
		req.body = []byte{}
	}
	return req, ok
}

func respond(out httptypes.ResponseOutparam, r response) {
	headers := httptypes.NewFields()
	add := func(name, value string) {
		headers.Append(httptypes.FieldName(name), httptypes.FieldValue(cm.ToList([]byte(value))))
	}
	add("content-type", r.contentType)
	add("content-length", strconv.Itoa(len(r.body)))
	if r.etag != "" {
		add("etag", r.etag)
	}
	resp := httptypes.NewOutgoingResponse(headers)
	resp.SetStatusCode(httptypes.StatusCode(r.status))
	bodyResult := resp.Body()
	outBody := *bodyResult.OK()
	httptypes.ResponseOutparamSet(out, cm.OK[cm.Result[httptypes.ErrorCodeShape, httptypes.OutgoingResponse, httptypes.ErrorCode]](resp))
	streamResult := outBody.Write()
	stream := *streamResult.OK()
	// blocking-write-and-flush takes at most 4096 bytes per call.
	for rest := r.body; len(rest) > 0; {
		n := min(len(rest), 4096)
		stream.BlockingWriteAndFlush(cm.ToList(rest[:n]))
		rest = rest[n:]
	}
	stream.ResourceDrop()
	httptypes.OutgoingBodyFinish(outBody, cm.None[httptypes.Trailers]())
}

func main() {}
