// The actor's hooks, in standard Go.
package export_wit_world

import (
	"wit_component/wasmkeep_actor_storage"
	"wit_component/wasmkeep_actor_websockets"

	witTypes "go.bytecodealliance.org/pkg/wit/types"
)

type hook = witTypes.Result[witTypes.Unit, string]

func OnInit() hook {
	for _, sql := range []string{
		"CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)",
		"INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)",
	} {
		if r := wasmkeep_actor_storage.Exec(sql, nil); r.IsErr() {
			return witTypes.Err[witTypes.Unit](r.Err().Message)
		}
	}
	return witTypes.Ok[witTypes.Unit, string](witTypes.Unit{})
}

func OnAlarm() hook { return witTypes.Ok[witTypes.Unit, string](witTypes.Unit{}) }

func OnWebsocketOpen(conn uint64, path string) hook {
	return witTypes.Err[witTypes.Unit]("no WebSockets here")
}

func OnWebsocketMessage(conn uint64, msg wasmkeep_actor_websockets.Message) {}

func OnWebsocketClose(conn uint64, code uint16, reason string) {}
