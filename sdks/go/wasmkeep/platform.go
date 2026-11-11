package wasmkeep

import (
	"go.bytecodealliance.org/cm"

	"github.com/wasmkeep/wasmkeep/sdks/go/call"
	wallclock "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/clocks/wall-clock"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasmkeep/actor/actors"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasmkeep/actor/alarms"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasmkeep/actor/env"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasmkeep/actor/websockets"
)

// NowMs is milliseconds since the Unix epoch, from the host's wall clock:
// the unit [SetAlarm] takes.
func NowMs() int64 {
	now := wallclock.Now()
	return int64(now.Seconds)*1000 + int64(now.Nanoseconds/1_000_000)
}

// ------------------------------------------------------------------ alarms

// SetAlarm schedules the Alarm hook at at (milliseconds since the Unix
// epoch), replacing any pending alarm. A time in the past fires as soon as
// possible. Alarms survive restarts and fire at least once.
func SetAlarm(at int64) {
	if at < 0 {
		at = 0
	}
	alarms.SetAlarm(uint64(at))
}

// GetAlarm is the pending alarm time, and whether there is one.
func GetAlarm() (int64, bool) {
	at := alarms.GetAlarm()
	if at.None() {
		return 0, false
	}
	return int64(at.Value()), true
}

// ClearAlarm cancels the pending alarm, if any.
func ClearAlarm() { alarms.ClearAlarm() }

// --------------------------------------------------------------------- env

// Env is the class's configuration value or secret name (`deploy
// --config`, `secrets set`, `serve --env`; last wins), and whether it is
// set. Values change only between events.
func Env(name string) (string, bool) {
	v := env.Get(name)
	if v.None() {
		return "", false
	}
	return v.Value(), true
}

// ---------------------------------------------------------------- actors

// Address is an actor's address. Within a tenant, Class + "/" + Name is
// what [Call] takes.
type Address struct {
	Tenant string
	Class  string
	Name   string
}

// Self is this actor's own address: requests arrive with
// `/actor/<class>/<name>` removed, and alarms carry nothing, so this is
// how an actor learns who it is.
func Self() Address {
	a := actors.SelfAddress()
	return Address{Tenant: a.Tenant, Class: a.Class, Name: a.Name}
}

// CallRequest is a request to another actor. An empty Method is GET, an
// empty Path "/".
type CallRequest struct {
	Method  string
	Path    string
	Headers [][2]string
	Body    []byte
}

// Call sends req to the actor at address (`class/name`, same tenant) and
// waits for its response; other events to this actor wait meanwhile. The
// callee sees `x-wasmkeep-caller: <tenant>/<class>/<name>`. A failed call
// (no answer) is a [*call.Error]; an answer, whatever its status, is not
// an error.
func Call(address string, req CallRequest) (Response, error) {
	method, path := req.Method, req.Path
	if method == "" {
		method = "GET"
	}
	if path == "" {
		path = "/"
	}
	r := actors.Call(address, actors.Request{
		Method:  method,
		Path:    path,
		Headers: cm.ToList(req.Headers),
		Body:    cm.ToList(req.Body),
	})
	if e := r.Err(); e != nil {
		return Response{}, callError(address, e)
	}
	ok := r.OK()
	resp := Response{Status: ok.Status, Body: append([]byte(nil), ok.Body.Slice()...)}
	resp.Headers = append(resp.Headers, ok.Headers.Slice()...)
	return resp, nil
}

// Caller is the [call.Caller] the kv, queue and workflow clients send
// through: kv.New("kv", 8, wasmkeep.Caller).
func Caller(address, method, path string, headers [][2]string, body []byte) (uint16, []byte, error) {
	resp, err := Call(address, CallRequest{Method: method, Path: path, Headers: headers, Body: body})
	if err != nil {
		return 0, nil, err
	}
	return resp.Status, resp.Body, nil
}

var _ call.Caller = Caller

func callError(address string, e *actors.CallError) error {
	detail := ""
	if s := e.InvalidAddress(); s != nil {
		detail = *s
	} else if s := e.Failed(); s != nil {
		detail = *s
	}
	return call.Failure(address, e.String(), detail)
}

// ------------------------------------------------------------- websockets

// SendError is why a message was not queued: "unknown-connection" (closed,
// or another actor's) or "backpressure" (the client is not reading fast
// enough; the message was dropped).
type SendError struct{ Kind string }

func (e *SendError) Error() string { return "websocket send: " + e.Kind }

// Send queues a text message on the connection.
func Send(conn uint64, text string) error {
	return send(conn, websockets.MessageText(text))
}

// SendBinary queues a binary message on the connection.
func SendBinary(conn uint64, data []byte) error {
	return send(conn, websockets.MessageBinary(cm.ToList(data)))
}

func send(conn uint64, msg websockets.Message) error {
	r := websockets.Send(conn, msg)
	if e := r.Err(); e != nil {
		return &SendError{Kind: e.String()}
	}
	return nil
}

// Broadcast sends text to every open connection of this actor except
// those in except; a connection that cannot take it is skipped.
func Broadcast(text string, except ...uint64) {
	msg := websockets.MessageText(text)
	for _, conn := range Connections() {
		if !contains(except, conn) {
			websockets.Send(conn, msg)
		}
	}
}

// BroadcastBinary is [Broadcast] with a binary message.
func BroadcastBinary(data []byte, except ...uint64) {
	msg := websockets.MessageBinary(cm.ToList(data))
	for _, conn := range Connections() {
		if !contains(except, conn) {
			websockets.Send(conn, msg)
		}
	}
}

// Close closes the connection with a close code (1000 is normal) and a
// reason.
func Close(conn uint64, code uint16, reason string) {
	websockets.Close(conn, code, reason)
}

// Connections is this actor's open connections, oldest first.
func Connections() []uint64 {
	return append([]uint64(nil), websockets.Connections().Slice()...)
}

func contains(list []uint64, v uint64) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}
