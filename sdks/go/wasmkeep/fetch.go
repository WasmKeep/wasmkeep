package wasmkeep

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"go.bytecodealliance.org/cm"

	outgoinghandler "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/http/outgoing-handler"
	httptypes "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/http/types"
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
// wall-clock limit (30 s by default). The host's egress rules apply.
func Fetch(url string, opts FetchOptions) (*Fetched, error) {
	target, err := parseURL(url)
	if err != nil {
		return nil, err
	}
	timeout, limit := opts.Timeout, opts.Limit
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if limit <= 0 {
		limit = 16 << 20
	}

	fields := httptypes.NewFields()
	sized := false
	for _, h := range opts.Headers {
		if !appendField(fields, h[0], h[1]) {
			fields.ResourceDrop()
			return nil, errors.New("header " + h[0] + ": not allowed or invalid")
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
		return nil, errors.New(url + ": invalid request")
	}
	bodyResult := request.Body()
	outgoing := bodyResult.OK()
	if outgoing == nil {
		request.ResourceDrop()
		return nil, errors.New("request body unavailable")
	}
	outBody := *outgoing

	options := httptypes.NewRequestOptions()
	nanos := httptypes.Duration(timeout.Nanoseconds())
	options.SetConnectTimeout(cm.Some(nanos))
	options.SetFirstByteTimeout(cm.Some(nanos))
	options.SetBetweenBytesTimeout(cm.Some(nanos))

	handled := outgoinghandler.Handle(request, cm.Some(options))
	if e := handled.Err(); e != nil {
		outBody.ResourceDrop()
		return nil, errors.New(e.String())
	}
	future := *handled.OK()
	defer future.ResourceDrop()

	var response httptypes.IncomingResponse
	sent := len(opts.Body) == 0 || writeAll(outBody, opts.Body)
	if sent {
		if r := httptypes.OutgoingBodyFinish(outBody, cm.None[httptypes.Trailers]()); r.IsErr() {
			return nil, errors.New(r.Err().String())
		}
		got, err := waitResponse(future)
		if err != nil {
			return nil, err
		}
		response = got
	} else {
		// The body's stream closes when the request ends before the guest
		// is done with it: either it failed, and the response says why, or
		// the server already answered and closed, and that answer is the
		// result.
		outBody.ResourceDrop()
		got, err := waitResponse(future)
		if err != nil {
			return nil, errors.New("sending the body: " + err.Error())
		}
		response = got
	}
	defer response.ResourceDrop()

	out := &Fetched{Status: uint16(response.Status())}
	headers := response.Headers()
	for _, e := range headers.Entries().Slice() {
		out.Headers = append(out.Headers, [2]string{string(e.F0), string(e.F1.Slice())})
	}
	headers.ResourceDrop()
	consumed := response.Consume()
	body := consumed.OK()
	if body == nil {
		return nil, errors.New(url + ": response body already taken")
	}
	out.Body, err = readResponseBody(*body, limit)
	if err != nil {
		return nil, errors.New(url + ": " + err.Error())
	}
	return out, nil
}

func waitResponse(future httptypes.FutureIncomingResponse) (httptypes.IncomingResponse, error) {
	for {
		got := future.Get()
		if got.None() {
			p := future.Subscribe()
			p.Block()
			p.ResourceDrop()
			continue
		}
		outer := got.Value()
		inner := outer.OK()
		if inner == nil {
			return 0, errors.New("response already taken")
		}
		if e := inner.Err(); e != nil {
			return 0, errors.New(e.String())
		}
		return *inner.OK(), nil
	}
}

// readResponseBody reads up to limit bytes; past the limit nothing more is
// read (nor checked). Before it, the body must end normally: a stream that
// breaks, or an error reported with the trailers, is an incomplete body.
func readResponseBody(body httptypes.IncomingBody, limit int) ([]byte, error) {
	streamResult := body.Stream()
	stream := streamResult.OK()
	if stream == nil {
		body.ResourceDrop()
		return nil, errors.New("incomplete body: its stream was already taken")
	}
	out := []byte{}
	for len(out) < limit {
		want := min(limit-len(out), 65536)
		chunk := stream.BlockingRead(uint64(want))
		if e := chunk.Err(); e != nil {
			if e.Closed() {
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
			stream.ResourceDrop()
			body.ResourceDrop()
			return nil, errors.New("incomplete body: " + why)
		}
		out = append(out, chunk.OK().Slice()...)
	}
	stream.ResourceDrop()
	if len(out) >= limit {
		// The rest is dropped unread.
		body.ResourceDrop()
		return out, nil
	}
	// Closed: the body's own end, unless the trailers say otherwise.
	trailers := httptypes.IncomingBodyFinish(body)
	defer trailers.ResourceDrop()
	for {
		got := trailers.Get()
		if got.None() {
			p := trailers.Subscribe()
			p.Block()
			p.ResourceDrop()
			continue
		}
		outer := got.Value()
		inner := outer.OK()
		if inner == nil {
			return nil, errors.New("incomplete body: its trailers were already taken")
		}
		if e := inner.Err(); e != nil {
			return nil, errors.New("incomplete body: " + e.String())
		}
		if t := inner.OK(); !t.None() {
			t.Value().ResourceDrop()
		}
		return out, nil
	}
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
