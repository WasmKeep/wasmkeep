package wasmkeep

import (
	"strconv"

	"go.bytecodealliance.org/cm"

	httptypes "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/http/types"
)

// Request is an incoming HTTP request, or a call from another actor. Path
// is relative to the actor (`/actor/<class>/<name>` removed) and includes
// the query.
type Request struct {
	Method string
	Path   string
	// Headers in the order they arrived, names in lower case.
	Headers [][2]string

	handle   httptypes.IncomingRequest
	live     bool
	body     []byte
	bodyErr  error
	bodyRead bool
}

// Header is the first value of the header name (any case), or "".
func (r *Request) Header(name string) string {
	v, _ := r.LookupHeader(name)
	return v
}

// LookupHeader is the first value of the header name (any case), and
// whether the request has it.
func (r *Request) LookupHeader(name string) (string, bool) {
	for _, h := range r.Headers {
		if equalFold(h[0], name) {
			return h[1], true
		}
	}
	return "", false
}

// Body is the whole request body, read on first use; what arrived of it
// if it broke off (see [Request.ReadBody]).
func (r *Request) Body() []byte {
	b, _ := r.ReadBody()
	return b
}

// ReadBody is the whole request body, read on first use, or an error if
// the body broke off before its end (the client went away).
func (r *Request) ReadBody() ([]byte, error) {
	if !r.bodyRead {
		r.bodyRead = true
		if r.live && readIncoming != nil {
			r.body, r.bodyErr = readIncoming(r.handle)
		}
	}
	return r.body, r.bodyErr
}

// Text is the body as a string.
func (r *Request) Text() string { return string(r.Body()) }

// NewRequest makes a request for a test of a Fetch handler, with body as
// its body.
func NewRequest(method, path string, headers [][2]string, body []byte) *Request {
	return &Request{Method: method, Path: path, Headers: headers, body: body, bodyRead: true}
}

// Response is what Fetch answers.
type Response struct {
	Status uint16
	// Headers sent in this order. Without a content-type, the response is
	// `text/plain; charset=utf-8`.
	Headers [][2]string
	Body    []byte
}

// Text is a response with a text body.
func Text(status uint16, body string) Response {
	return Response{Status: status, Body: []byte(body)}
}

// Bytes is a response with body and its content type.
func Bytes(status uint16, contentType string, body []byte) Response {
	return Response{Status: status, Headers: [][2]string{{"content-type", contentType}}, Body: body}
}

// JSON is a response whose body is already JSON text.
func JSON(status uint16, json string) Response {
	return Bytes(status, "application/json", []byte(json))
}

// WithHeader adds a header to the response.
func (r Response) WithHeader(name, value string) Response {
	r.Headers = append(r.Headers, [2]string{name, value})
	return r
}

// readIncoming reads a request's whole body (set when built for Wasm).
var readIncoming func(httptypes.IncomingRequest) ([]byte, error)

func incoming(req httptypes.IncomingRequest) *Request {
	r := &Request{handle: req, live: true, Method: methodName(req.Method()), Path: "/"}
	if p := req.PathWithQuery(); !p.None() {
		r.Path = p.Value()
	}
	headers := req.Headers()
	for _, e := range headers.Entries().Slice() {
		r.Headers = append(r.Headers, [2]string{string(e.F0), string(e.F1.Slice())})
	}
	headers.ResourceDrop()
	return r
}

func (r *Request) release() {
	if r.live {
		r.live = false
		r.handle.ResourceDrop()
	}
}

var methodNames = [...]string{"GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH"}

func methodName(m httptypes.Method) string {
	if o := m.Other(); o != nil {
		return *o
	}
	if int(m.Tag()) < len(methodNames) {
		return methodNames[m.Tag()]
	}
	return "GET"
}

func respond(out httptypes.ResponseOutparam, r Response) {
	headers := httptypes.NewFields()
	typed, sized := false, false
	for _, h := range r.Headers {
		typed = typed || equalFold(h[0], "content-type")
		sized = sized || equalFold(h[0], "content-length")
	}
	if !typed {
		appendField(headers, "content-type", "text/plain; charset=utf-8")
	}
	for _, h := range r.Headers {
		appendField(headers, h[0], h[1])
	}
	if !sized {
		appendField(headers, "content-length", strconv.Itoa(len(r.Body)))
	}
	resp := httptypes.NewOutgoingResponse(headers)
	status := r.Status
	if status == 0 {
		status = 200
	}
	resp.SetStatusCode(httptypes.StatusCode(status))
	bodyResult := resp.Body()
	body := bodyResult.OK()
	httptypes.ResponseOutparamSet(out, cm.OK[cm.Result[httptypes.ErrorCodeShape, httptypes.OutgoingResponse, httptypes.ErrorCode]](resp))
	if body == nil {
		return
	}
	writeAll(*body, r.Body)
	httptypes.OutgoingBodyFinish(*body, cm.None[httptypes.Trailers]())
}

// writeAll writes data to the body's stream; false if the stream broke.
func writeAll(body httptypes.OutgoingBody, data []byte) bool {
	streamResult := body.Write()
	stream := streamResult.OK()
	if stream == nil {
		return false
	}
	defer stream.ResourceDrop()
	// blocking-write-and-flush takes at most 4096 bytes at a time.
	for at := 0; at < len(data); at += 4096 {
		end := min(at+4096, len(data))
		if r := stream.BlockingWriteAndFlush(cm.ToList(data[at:end])); r.IsErr() {
			return false
		}
	}
	return true
}

func appendField(f httptypes.Fields, name, value string) bool {
	r := f.Append(httptypes.FieldName(name), httptypes.FieldValue(cm.ToList([]byte(value))))
	return !r.IsErr()
}

// equalFold compares ASCII header names regardless of case.
func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		x, y := a[i], b[i]
		if 'A' <= x && x <= 'Z' {
			x += 'a' - 'A'
		}
		if 'A' <= y && y <= 'Z' {
			y += 'a' - 'A'
		}
		if x != y {
			return false
		}
	}
	return true
}
