// A chat room in Go (TinyGo), with the WasmKeep Go SDK (sdks/go). Each
// actor name is one room: the Rust examples/chat-room, line for line in
// behaviour.
//
//   - Connect a WebSocket to /actor/<class>/<room>: you receive the last
//     20 messages, then everything said in the room. Text you send is
//     broadcast to everyone in the room, you included. If the operator set
//     MOTD (wasmkeep serve --env chat=FILE), it comes first, as
//     "* <MOTD>".
//   - POST / with a text body says something over plain HTTP.
//   - GET / lists the recent messages.
//
// Messages are kept in the room's SQLite database (the last 100). The host
// owns the sockets, so the room can hibernate while people stay
// connected: the next message wakes it.
package main

import (
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/wasmkeep/wasmkeep/sdks/go/wasmkeep"
)

const (
	history = 20
	keep    = 100
	recent  = "SELECT text FROM (SELECT id, text FROM messages ORDER BY id DESC LIMIT ?) ORDER BY id"
)

func init() {
	wasmkeep.Serve(wasmkeep.Actor{
		Init: func() error {
			_, err := wasmkeep.Exec("CREATE TABLE IF NOT EXISTS messages" +
				" (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, text TEXT NOT NULL)")
			return err
		},
		Fetch:   fetch,
		Open:    open,
		Message: message,
		Close: func(uint64, uint16, string) {
			announce("left")
		},
	})
}

func say(text string) error {
	if _, err := wasmkeep.Exec("INSERT INTO messages (at, text) VALUES (?, ?)", wasmkeep.NowMs(), text); err != nil {
		return err
	}
	if _, err := wasmkeep.Exec("DELETE FROM messages WHERE id <= (SELECT max(id) FROM messages) - ?", keep); err != nil {
		return err
	}
	wasmkeep.Broadcast(text)
	return nil
}

// announce says "* someone joined #lobby (2 online)": the room is this
// actor's name.
func announce(what string) {
	online := strconv.Itoa(len(wasmkeep.Connections()))
	wasmkeep.Broadcast("* someone " + what + " #" + wasmkeep.Self().Name + " (" + online + " online)")
}

func open(conn uint64, _ string) error {
	if motd, ok := wasmkeep.Env("MOTD"); ok {
		wasmkeep.Send(conn, "* "+motd)
	}
	rows, err := wasmkeep.Query(recent, history)
	if err != nil {
		return err
	}
	for i := 0; i < rows.Len(); i++ {
		wasmkeep.Send(conn, rows.String(i, 0))
	}
	announce("joined")
	return nil
}

func message(_ uint64, data []byte, binary bool) {
	if !binary {
		say(string(data))
	}
}

func fetch(req *wasmkeep.Request) wasmkeep.Response {
	switch req.Method {
	case "POST":
		body := req.Body()
		text := ""
		if len(body) <= 4096 && utf8.Valid(body) {
			text = strings.Trim(string(body), " \t\n\v\f\r")
		}
		if text == "" {
			return wasmkeep.Text(400, "send a UTF-8 text body\n")
		}
		if err := say(text); err != nil {
			return wasmkeep.Text(500, err.Error()+"\n")
		}
		return wasmkeep.Text(200, "sent\n")
	case "GET":
		rows, err := wasmkeep.Query(recent, history)
		if err != nil {
			return wasmkeep.Text(500, err.Error()+"\n")
		}
		var b strings.Builder
		for i := 0; i < rows.Len(); i++ {
			if i > 0 {
				b.WriteByte('\n')
			}
			b.WriteString(rows.String(i, 0))
		}
		b.WriteByte('\n')
		return wasmkeep.Text(200, b.String())
	}
	return wasmkeep.Text(405, "use GET or POST\n")
}

func main() {}
