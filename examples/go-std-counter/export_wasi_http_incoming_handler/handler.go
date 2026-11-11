// The counter's requests: GET reads it, POST increments it.
package export_wasi_http_incoming_handler

import (
	"strconv"

	"wit_component/wasi_http_types"
	"wit_component/wasmkeep_actor_storage"

	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

func query(sql string) (int64, string) {
	r := wasmkeep_actor_storage.Query(sql, nil)
	if r.IsErr() {
		return 0, r.Err().Message
	}
	rows := r.Ok()
	if len(rows.Values) == 0 {
		return 0, "no row"
	}
	return rows.Values[0][0].Integer(), ""
}

func Handle(req *wasi_http_types.IncomingRequest, out *wasi_http_types.ResponseOutparam) {
	status, body := uint16(200), ""
	switch req.Method().Tag() {
	case wasi_http_types.Method_Get:
		n, err := query("SELECT value FROM counter WHERE id = 1")
		body = strconv.FormatInt(n, 10) + "\n"
		if err != "" {
			status, body = 500, err+"\n"
		}
	case wasi_http_types.Method_Post:
		n, err := query("UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value")
		body = strconv.FormatInt(n, 10) + "\n"
		if err != "" {
			status, body = 500, err+"\n"
		}
	default:
		status, body = 405, "use GET or POST\n"
	}
	req.Drop()
	headers := wasi_http_types.MakeFields()
	headers.Append("content-type", []byte("text/plain"))
	resp := wasi_http_types.MakeOutgoingResponse(headers)
	resp.SetStatusCode(status)
	outBody := resp.Body().Ok()
	wasi_http_types.ResponseOutparamSet(out, witTypes.Ok[*wasi_http_types.OutgoingResponse, wasi_http_types.ErrorCode](resp))
	stream := outBody.Write().Ok()
	stream.BlockingWriteAndFlush([]byte(body))
	stream.Drop()
	wasi_http_types.OutgoingBodyFinish(outBody, witTypes.None[*wasi_http_types.Fields]())
}
