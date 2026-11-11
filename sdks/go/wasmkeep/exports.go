//go:build tinygo.wasm || wasm

package wasmkeep

// The component's exports, registered only when built for Wasm (TinyGo's
// wasip2 target), and reading a request's body: on the host, `go test`
// links without the host's functions.

import (
	"errors"

	"go.bytecodealliance.org/cm"

	incominghandler "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/http/incoming-handler"
	httptypes "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasi/http/types"
	"github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasmkeep/actor/actor"
	actorcore "github.com/wasmkeep/wasmkeep/sdks/go/internal/gen/wasmkeep/actor/actor-core"
)

type hookResult = cm.Result[string, struct{}, string]

func hookOK() hookResult { return cm.OK[hookResult](struct{}{}) }

func hookErr(err error) hookResult {
	msg := err.Error()
	if msg == "" {
		msg = "failed"
	}
	return cm.Err[hookResult](msg)
}

func init() {
	readIncoming = readBody
	actor.Exports.OnInit = func() hookResult {
		if current.Init == nil {
			return hookOK()
		}
		if err := current.Init(); err != nil {
			return hookErr(err)
		}
		return hookOK()
	}
	actor.Exports.OnAlarm = func() hookResult {
		if current.Alarm == nil {
			return hookOK()
		}
		if err := current.Alarm(); err != nil {
			return hookErr(err)
		}
		return hookOK()
	}
	actor.Exports.OnWebsocketOpen = func(conn uint64, path string) hookResult {
		if current.Open == nil {
			if current.Message == nil {
				return cm.Err[hookResult]("this actor takes no WebSockets")
			}
			return hookOK()
		}
		if err := current.Open(conn, path); err != nil {
			return hookErr(err)
		}
		return hookOK()
	}
	actor.Exports.OnWebsocketMessage = func(conn uint64, msg actorcore.Message) {
		if current.Message == nil {
			return
		}
		if t := msg.Text(); t != nil {
			current.Message(conn, []byte(*t), false)
		} else if b := msg.Binary(); b != nil {
			current.Message(conn, b.Slice(), true)
		}
	}
	actor.Exports.OnWebsocketClose = func(conn uint64, code uint16, reason string) {
		if current.Close != nil {
			current.Close(conn, code, reason)
		}
	}
	incominghandler.Exports.Handle = func(req httptypes.IncomingRequest, out httptypes.ResponseOutparam) {
		r := incoming(req)
		var resp Response
		if current.Fetch == nil {
			resp = Text(404, "no fetch handler\n")
		} else {
			resp = current.Fetch(r)
		}
		r.release()
		respond(out, resp)
	}
}

// errIncomplete is a request body that broke off.
var errIncomplete = errors.New("the request body broke off before its end")

// readBody reads a request's whole body.
func readBody(req httptypes.IncomingRequest) ([]byte, error) {
	consumed := req.Consume()
	body := consumed.OK()
	if body == nil {
		return []byte{}, errIncomplete
	}
	streamResult := body.Stream()
	stream := streamResult.OK()
	if stream == nil {
		body.ResourceDrop()
		return []byte{}, errIncomplete
	}
	out := []byte{}
	var err error
	for {
		chunk := stream.BlockingRead(65536)
		if e := chunk.Err(); e != nil {
			if !e.Closed() { // closed is the body's end
				err = errIncomplete
			}
			break
		}
		out = append(out, chunk.OK().Slice()...)
	}
	stream.ResourceDrop()
	httptypes.IncomingBodyFinish(*body).ResourceDrop()
	return out, err
}
