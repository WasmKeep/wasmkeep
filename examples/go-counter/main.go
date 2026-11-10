// A counter actor in Go (TinyGo), with the WasmKeep Go SDK (sdks/go). GET
// returns the value, POST increments it; each actor name is its own
// counter, kept in its SQLite database.
package main

import (
	"strconv"

	"github.com/wasmkeep/wasmkeep/sdks/go/wasmkeep"
)

func init() {
	wasmkeep.Serve(wasmkeep.Actor{Init: migrate, Fetch: fetch})
}

func migrate() error {
	if _, err := wasmkeep.Exec("CREATE TABLE IF NOT EXISTS counter" +
		" (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)"); err != nil {
		return err
	}
	_, err := wasmkeep.Exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)")
	return err
}

func fetch(req *wasmkeep.Request) wasmkeep.Response {
	var sql string
	switch req.Method {
	case "GET":
		sql = "SELECT value FROM counter WHERE id = 1"
	case "POST":
		sql = "UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value"
	default:
		return wasmkeep.Text(405, "use GET or POST\n")
	}
	value, err := wasmkeep.QueryInt(sql)
	if err != nil {
		return wasmkeep.Text(500, err.Error()+"\n")
	}
	return wasmkeep.Text(200, strconv.FormatInt(value, 10)+"\n")
}

func main() {}
