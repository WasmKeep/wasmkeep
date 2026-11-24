package wasmkeep

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"go.bytecodealliance.org/cm"

	monotonicclock "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/clocks/monotonic-clock"
	outgoinghandler "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/http/outgoing-handler"
	httptypes "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/http/types"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/io/poll"
)

// FetchOptions is what [Fetch] sends. Every field is optional.
type FetchOptions struct {
	// Method is GET unless given.
	Method  string
	Headers [][2]string
	Body    []byte
	// Timeout bounds connecting, the first byte of the response and each
	// gap between bytes; 10 s unless given.
	Timeout time.Duration
	// Limit is how many bytes of the response body are kept (the rest is
	// dropped unread, and the response still succeeds); 16 MiB unless
	// given.
	Limit int
}

// Fetched is a response to [Fetch].
type Fetched struct {
	Status uint16
	// Headers in the order they arrived, names in lower case.
	Headers [][2]string
	// Body is the whole body, or its first Limit bytes.
	Body []byte
}

// Header is the first value of the header name (any case), or "".
func (f *Fetched) Header(name string) string {
	for _, h := range f.Headers {
		if equalFold(h[0], name) {
			return h[1]
		}
	}
	return ""
}

// Fetch sends an outbound HTTP request to url (`http://` or `https://`)
// and waits for the response, as the Rust SDK's `http::fetch` does.
// Errors are described as text (refused, timeout, ...), ready to log. A
// body that breaks before its end and before the limit (the connection
// closes short of its Content-Length or inside a chunk, a reset, a timeout
// between bytes) is an error starting `<url>: incomplete body:`, not the
// status with the bytes that came.
//
// The actor's event waits meanwhile (other events to it queue); waiting
// does not count against its CPU budget, but does against the event's
// wall-clock limit (30 s by default). The host's egress rules apply. For
// several requests at once, see [FetchAll]: goroutines do not help, since
// a goroutine waiting for its response blocks the whole component.
func Fetch(url string, opts FetchOptions) (*Fetched, error) {
	r := FetchAll([]FetchRequest{{URL: url, Options: opts}})[0]
	return r.Fetched, r.Err
}

// FetchRequest is one request of a [FetchAll] batch.
type FetchRequest struct {
	URL     string
	Options FetchOptions
}

// FetchResult is what one request of a [FetchAll] batch gave: a response,
// or an error, as [Fetch] returns them.
type FetchResult struct {
	Fetched *Fetched
	Err     error
}

// maxOpen is how many requests a batch keeps open at once: the host's
// limit per actor (PER_ACTOR_REQUESTS in the server's egress rules).
const maxOpen = 16

// A request the host refused for having too many open is tried again up
// to retries times, retryWait apart: the host frees a finished request's
// place a moment after its body ends.
const (
	retries   = 100
	retryWait = monotonicclock.Duration(time.Millisecond)
)

// FetchAll sends every request at once and waits for all of them: one
// result per request, in the same order, each as [Fetch] gives it. One
// failing does not fail the others.
//
// The requests are open together, their responses and bodies read as they
// come, so the batch takes about as long as its slowest request rather
// than the sum. Request bodies are written one after the other, once every
// request that fits has started. The host lets an actor have 16
// requests open at once: past 16, the rest start as earlier ones finish.
// The host's egress rules apply to each.
func FetchAll(requests []FetchRequest) []FetchResult {
	items := make([]*batchItem, len(requests))
	for i, r := range requests {
		items[i] = &batchItem{url: r.URL, opts: r.Options}
	}
	waited := false // a retry wait has just passed
	for {
		// Start what fits: every `handle` first, then the bodies.
		open := 0
		for _, it := range items {
			if it.isOpen() {
				open++
			}
		}
		var started []*batchItem
		for _, it := range items {
			if open >= maxOpen {
				break
			}
			if it.phase != phaseQueued || (it.tries > 0 && !waited) {
				continue
			}
			if it.start() {
				open++
				started = append(started, it)
			}
		}
		for _, it := range started {
			it.sendBody()
		}

		// Advance each as far as it goes without waiting.
		retrying := false
		for _, it := range items {
			it.advance()
			if it.phase == phaseQueued && it.tries > 0 {
				retrying = true
			}
		}
		pending := false
		for _, it := range items {
			pending = pending || it.phase != phaseDone
		}
		if !pending {
			break
		}
		// Wait for any of them, and for a moment when one is to be retried
		// (then something is always waited for: polling nothing traps).
		var pollables []poll.Pollable
		for _, it := range items {
			if p, ok := it.pollable(); ok {
				pollables = append(pollables, p)
			}
		}
		// Every request that was open is done, and others are still to
		// start: there is nothing to wait for, so start them.
		if len(pollables) == 0 && !retrying {
			waited = false
			continue
		}
		clock := -1
		if retrying {
			pollables = append(pollables, monotonicclock.SubscribeDuration(retryWait))
			clock = len(pollables) - 1
		}
		ready := poll.Poll(cm.ToList(pollables))
		waited = false
		for _, r := range ready.Slice() {
			waited = waited || int(r) == clock
		}
		// Pollables are their resources' children: dropped before any of
		// those changes.
		for _, p := range pollables {
			p.ResourceDrop()
		}
	}
	out := make([]FetchResult, len(items))
	for i, it := range items {
		out[i] = it.result
	}
	return out
}

type phase int

const (
	phaseQueued   phase = iota
	phaseHead           // waiting for the response head
	phaseBody           // reading the body
	phaseTrailers       // the body ended: waiting for the trailers
	phaseDone
)

// batchItem is one request of a batch, and the resources it holds: they
// are dropped each before the one it came from (see end).
type batchItem struct {
	url   string
	opts  FetchOptions
	phase phase
	// How many times the host refused it for having too many open.
	tries int

	outgoing httptypes.OutgoingBody
	future   httptypes.FutureIncomingResponse
	// Writing the request body failed.
	sendFailed bool
	response   httptypes.IncomingResponse
	fetched    *Fetched
	body       httptypes.IncomingBody
	stream     httptypes.InputStream
	trailers   httptypes.FutureTrailers
	limit      int

	result FetchResult
}

func (it *batchItem) isOpen() bool {
	return it.phase == phaseHead || it.phase == phaseBody || it.phase == phaseTrailers
}

// end drops what the request still holds, in order, and records result.
func (it *batchItem) end(fetched *Fetched, err error) {
	if it.phase == phaseBody {
		it.stream.ResourceDrop()
		it.body.ResourceDrop()
	}
	if it.phase == phaseTrailers {
		it.trailers.ResourceDrop()
	}
	if it.phase == phaseBody || it.phase == phaseTrailers {
		it.response.ResourceDrop()
	}
	if it.phase != phaseQueued {
		it.future.ResourceDrop()
	}
	it.phase = phaseDone
	it.result = FetchResult{Fetched: fetched, Err: err}
}

// start builds the request and hands it to the host; false (and done) if
// it could not.
func (it *batchItem) start() bool {
	url, opts := it.url, it.opts
	fail := func(err error) bool {
		it.phase = phaseDone
		it.result = FetchResult{Err: err}
		return false
	}
	target, err := parseURL(url)
	if err != nil {
		return fail(err)
	}
	timeout, limit := opts.Timeout, opts.Limit
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if limit <= 0 {
		limit = 16 << 20
	}
	it.limit = limit

	fields := httptypes.NewFields()
	sized := false
	for _, h := range opts.Headers {
		if !appendField(fields, h[0], h[1]) {
			fields.ResourceDrop()
			return fail(errors.New("header " + h[0] + ": not allowed or invalid"))
		}
		sized = sized || equalFold(h[0], "content-length")
	}
	// A known length rather than a chunked body: some servers refuse
	// requests without one.
	if len(opts.Body) > 0 && !sized {
		appendField(fields, "content-length", strconv.Itoa(len(opts.Body)))
	}
	request := httptypes.NewOutgoingRequest(fields)
	scheme := httptypes.SchemeHTTP()
	if target.https {
		scheme = httptypes.SchemeHTTPS()
	}
	r1 := request.SetMethod(outgoingMethod(opts.Method))
	r2 := request.SetScheme(cm.Some(scheme))
	r3 := request.SetAuthority(cm.Some(target.authority))
	r4 := request.SetPathWithQuery(cm.Some(target.path))
	if r1|r2|r3|r4 != cm.ResultOK {
		request.ResourceDrop()
		return fail(errors.New(url + ": invalid request"))
	}
	bodyResult := request.Body()
	outgoing := bodyResult.OK()
	if outgoing == nil {
		request.ResourceDrop()
		return fail(errors.New("request body unavailable"))
	}

	options := httptypes.NewRequestOptions()
	nanos := httptypes.Duration(timeout.Nanoseconds())
	options.SetConnectTimeout(cm.Some(nanos))
	options.SetFirstByteTimeout(cm.Some(nanos))
	options.SetBetweenBytesTimeout(cm.Some(nanos))

	handled := outgoinghandler.Handle(request, cm.Some(options))
	if e := handled.Err(); e != nil {
		outgoing.ResourceDrop()
		return fail(errors.New(e.String()))
	}
	it.outgoing = *outgoing
	it.future = *handled.OK()
	it.phase = phaseHead
	return true
}

// sendBody writes the request body and finishes it.
func (it *batchItem) sendBody() {
	if len(it.opts.Body) == 0 || writeAll(it.outgoing, it.opts.Body) {
		if r := httptypes.OutgoingBodyFinish(it.outgoing, cm.None[httptypes.Trailers]()); r.IsErr() {
			it.end(nil, errors.New(r.Err().String()))
		}
		return
	}
	// The body's stream closes when the request ends before the guest is
	// done with it: either it failed, and the response says why, or the
	// server already answered and closed, and that answer is the result.
	it.outgoing.ResourceDrop()
	it.sendFailed = true
}

// fail ends the request with err, prefixed as Fetch words it.
func (it *batchItem) fail(err string) {
	if it.sendFailed {
		err = "sending the body: " + err
	}
	it.end(nil, errors.New(err))
}

// advance moves the request on as far as it goes without waiting.
func (it *batchItem) advance() {
	for {
		switch it.phase {
		case phaseHead:
			got := it.future.Get()
			if got.None() {
				return
			}
			outer := got.Value()
			inner := outer.OK()
			if inner == nil {
				it.fail("response already taken")
				continue
			}
			if e := inner.Err(); e != nil {
				if e.ConnectionLimitReached() && it.tries < retries {
					it.future.ResourceDrop()
					it.phase = phaseQueued
					it.tries++
					return
				}
				it.fail(e.String())
				continue
			}
			it.response = *inner.OK()
			out := &Fetched{Status: uint16(it.response.Status())}
			headers := it.response.Headers()
			for _, e := range headers.Entries().Slice() {
				out.Headers = append(out.Headers, [2]string{string(e.F0), string(e.F1.Slice())})
			}
			headers.ResourceDrop()
			consumed := it.response.Consume()
			body := consumed.OK()
			if body == nil {
				it.response.ResourceDrop()
				it.fail(it.url + ": response body already taken")
				continue
			}
			streamResult := body.Stream()
			stream := streamResult.OK()
			if stream == nil {
				body.ResourceDrop()
				it.response.ResourceDrop()
				it.fail(it.url + ": incomplete body: its stream was already taken")
				continue
			}
			out.Body = []byte{}
			it.fetched, it.body, it.stream = out, *body, *stream
			it.phase = phaseBody
		case phaseBody:
			out := it.fetched
			ended := false
			for len(out.Body) < it.limit {
				want := min(it.limit-len(out.Body), 65536)
				chunk := it.stream.Read(uint64(want))
				if e := chunk.Err(); e != nil {
					if e.Closed() {
						ended = true
						break
					}
					why := "stream error"
					if f := e.LastOperationFailed(); f != nil {
						if code := httptypes.HTTPErrorCode(*f); !code.None() {
							why = code.Value().String()
						} else {
							why = f.ToDebugString()
						}
						f.ResourceDrop()
					}
					it.end(nil, errors.New(it.url+": incomplete body: "+why))
					return
				}
				data := chunk.OK().Slice()
				if len(data) == 0 {
					return // nothing yet
				}
				out.Body = append(out.Body, data...)
			}
			if !ended {
				// At the limit: the rest is dropped unread.
				it.end(out, nil)
				return
			}
			// Closed: the body's own end, unless the trailers say otherwise.
			it.stream.ResourceDrop()
			it.trailers = httptypes.IncomingBodyFinish(it.body)
			it.phase = phaseTrailers
		case phaseTrailers:
			got := it.trailers.Get()
			if got.None() {
				return
			}
			outer := got.Value()
			inner := outer.OK()
			switch {
			case inner == nil:
				it.end(nil, errors.New(it.url+": incomplete body: its trailers were already taken"))
			case inner.Err() != nil:
				it.end(nil, errors.New(it.url+": incomplete body: "+inner.Err().String()))
			default:
				// Trailers received are the future's child: dropped first.
				if t := inner.OK(); !t.None() {
					t.Value().ResourceDrop()
				}
				it.end(it.fetched, nil)
			}
			return
		default:
			return
		}
	}
}

// pollable is what the request waits for, if anything.
func (it *batchItem) pollable() (poll.Pollable, bool) {
	switch it.phase {
	case phaseHead:
		return it.future.Subscribe(), true
	case phaseBody:
		return it.stream.Subscribe(), true
	case phaseTrailers:
		return it.trailers.Subscribe(), true
	}
	return 0, false
}

func outgoingMethod(m string) httptypes.Method {
	switch strings.ToUpper(m) {
	case "", "GET":
		return httptypes.MethodGet()
	case "HEAD":
		return httptypes.MethodHead()
	case "POST":
		return httptypes.MethodPost()
	case "PUT":
		return httptypes.MethodPut()
	case "DELETE":
		return httptypes.MethodDelete()
	case "PATCH":
		return httptypes.MethodPatch()
	case "OPTIONS":
		return httptypes.MethodOptions()
	}
	return httptypes.MethodOther(strings.ToUpper(m))
}

type target struct {
	https     bool
	authority string
	path      string
}

// parseURL splits url into its scheme, authority and path with query, as
// the Rust SDK does.
func parseURL(url string) (target, error) {
	scheme, rest, ok := strings.Cut(url, "://")
	if !ok {
		return target{}, errors.New(url + ": no scheme")
	}
	var t target
	switch scheme {
	case "http":
	case "https":
		t.https = true
	default:
		return target{}, errors.New(url + ": unsupported scheme " + scheme)
	}
	if i := strings.IndexAny(rest, "/?"); i >= 0 {
		t.authority, t.path = rest[:i], rest[i:]
	} else {
		t.authority, t.path = rest, "/"
	}
	if !strings.HasPrefix(t.path, "/") {
		t.path = "/" + t.path
	}
	if t.authority == "" {
		return target{}, errors.New(url + ": no host")
	}
	return t, nil
}
