// A counter actor in MoonBit, with the WasmKeep MoonBit SDK
// (sdks/moonbit). GET returns the value, POST increments it; each actor
// name is its own counter, kept in its SQLite database.

///|
pub let actor : @wasmkeep.Actor = @wasmkeep.Actor::new(init=migrate, fetch~)

///|
fn migrate() -> Unit raise {
  @wasmkeep.exec(
    "CREATE TABLE IF NOT EXISTS counter (id INTEGER PRIMARY KEY CHECK (id = 1), value INTEGER NOT NULL)",
  )
  |> ignore
  @wasmkeep.exec("INSERT OR IGNORE INTO counter (id, value) VALUES (1, 0)") |> ignore
}

///|
fn fetch(req : @wasmkeep.Request) -> @wasmkeep.Response raise {
  let sql = match req.method_ {
    "GET" => "SELECT value FROM counter WHERE id = 1"
    "POST" => "UPDATE counter SET value = value + 1 WHERE id = 1 RETURNING value"
    _ => return @wasmkeep.reply(405, "use GET or POST\n")
  }
  @wasmkeep.reply(200, "\{@wasmkeep.query_int(sql)}\n")
}
