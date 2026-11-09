// A counter actor in Go (TinyGo), on the bindings wit-bindgen-go generates
// from the WasmKeep WIT. GET returns the value, POST increments it; each
// actor name is its own counter, kept in its SQLite database.
package main

import (
	"strconv"

	"go.bytecodealliance.org/cm"
	incominghandler "wasmkeep.example/go-counter/gen/wasi/http/incoming-handler"
	httptypes "wasmkeep.example/go-counter/gen/wasi/http/types"
	"wasmkeep.example/go-counter/gen/wasmkeep/actor/actor"
	actorcore "wasmkeep.example/go-counter/gen/wasmkeep/actor/actor-core"
	"wasmkeep.example/go-counter/gen/wasmkeep/actor/storage"
)

type hookResult = cm.Result[string, struct{}, string]

func init() {
	actor.Exports.OnInit = onInit
	actor.Exports.OnAlarm = func() hookResult { return cm.OK[hookResult](struct{}{}) }
	actor.Exports.OnWebsocketOpen = func(uint64, string) hookResult {
		return cm.Err[hookResult]("no WebSockets here")
	}
	actor.Exports.OnWebsocketMessage = func(uint64, actorcore.Message) {}
	actor.Exports.OnWebsocketClose = func(uint64, uint16, string) {}
	incominghandler.Exports.Handle = handle
}

func onInit() hookResult {
	for _, sql := range []string{
		"CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)",
		"INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)",
	} {
		if r := storage.Exec(sql, cm.List[storage.Value]{}); r.IsErr() {
			return cm.Err[hookResult](r.Err().Message)
		}
	}
	return cm.OK[hookResult](struct{}{})
}

// queryInt returns the first column of the first row as an integer.
func queryInt(sql string) (int64, string) {
	r := storage.Query(sql, cm.List[storage.Value]{})
	if r.IsErr() {
		return 0, r.Err().Message
	}
	rows := r.OK().Values.Slice()
	if len(rows) == 0 || rows[0].Len() == 0 {
		return 0, "no row"
	}
	if v := rows[0].Slice()[0].Integer(); v != nil {
		return *v, ""
	}
	return 0, "not an integer"
}

func handle(req httptypes.IncomingRequest, out httptypes.ResponseOutparam) {
	defer req.ResourceDrop()
	var value int64
	var err string
	method := req.Method()
	switch {
	case method.Get():
		value, err = queryInt("SELECT value FROM counter WHERE id = 1")
	case method.Post():
		value, err = queryInt("UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value")
	default:
		respond(out, 405, "use GET or POST\n")
		return
	}
	if err != "" {
		respond(out, 500, err+"\n")
		return
	}
	respond(out, 200, strconv.FormatInt(value, 10)+"\n")
}

func respond(out httptypes.ResponseOutparam, status uint16, body string) {
	headers := httptypes.NewFields()
	headers.Append("content-type", httptypes.FieldValue(cm.ToList([]byte("text/plain"))))
	headers.Append("content-length", httptypes.FieldValue(cm.ToList([]byte(strconv.Itoa(len(body))))))
	resp := httptypes.NewOutgoingResponse(headers)
	resp.SetStatusCode(httptypes.StatusCode(status))
	bodyResult := resp.Body()
	outBody := bodyResult.OK()
	httptypes.ResponseOutparamSet(out, cm.OK[cm.Result[httptypes.ErrorCodeShape, httptypes.OutgoingResponse, httptypes.ErrorCode]](resp))
	streamResult := outBody.Write()
	stream := streamResult.OK()
	stream.BlockingWriteAndFlush(cm.ToList([]byte(body)))
	stream.ResourceDrop()
	httptypes.OutgoingBodyFinish(*outBody, cm.None[httptypes.Trailers]())
}

func main() {}
