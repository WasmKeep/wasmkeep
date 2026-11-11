// A chat room in MoonBit, with the WasmKeep MoonBit SDK (sdks/moonbit).
// Each actor name is one room: the Rust examples/chat-room, line for line
// in behaviour.
//
// - Connect a WebSocket to /actor/<class>/<room>: you receive the last 20
//   messages, then everything said in the room. Text you send is
//   broadcast to everyone in the room, you included. If the operator set
//   MOTD (wasmkeep serve --env chat=FILE), it comes first, as "* <MOTD>".
// - POST / with a text body says something over plain HTTP.
// - GET / lists the recent messages.
//
// Messages are kept in the room's SQLite database (the last 100). The
// host owns the sockets, so the room can hibernate while people stay
// connected: the next message wakes it.

///|
pub let actor : @wasmkeep.Actor = @wasmkeep.Actor::new(
  init=migrate,
  fetch~,
  open~,
  message~,
  close=fn(_conn, _code, _reason) { announce("left") },
)

///|
let recent = "SELECT text FROM (SELECT id, text FROM messages ORDER BY id DESC LIMIT ?) ORDER BY id"

///|
fn migrate() -> Unit raise {
  @wasmkeep.exec(
    "CREATE TABLE IF NOT EXISTS messages (id INTEGER PRIMARY KEY, at INTEGER NOT NULL, text TEXT NOT NULL)",
  )
  |> ignore
}

///|
fn say(text : String) -> Unit raise {
  @wasmkeep.exec("INSERT INTO messages (at, text) VALUES (?, ?)", params=[
    @wasmkeep.int(@wasmkeep.now_ms()),
    @wasmkeep.text(text),
  ])
  |> ignore
  @wasmkeep.exec("DELETE FROM messages WHERE id <= (SELECT max(id) FROM messages) - ?", params=[
    @wasmkeep.int(100),
  ])
  |> ignore
  @wasmkeep.broadcast(text)
}

///|
/// "* someone joined #lobby (2 online)": the room is this actor's name.
fn announce(what : String) -> Unit {
  let online = @wasmkeep.connections().length()
  @wasmkeep.broadcast("* someone \{what} #\{@wasmkeep.self_address().name} (\{online} online)")
}

///|
fn history() -> Array[String] raise {
  let rows = @wasmkeep.query(recent, params=[@wasmkeep.int(20)])
  Array::makei(rows.count(), fn(i) { rows.get_text(i, 0) })
}

///|
fn open(conn : UInt64, _path : String) -> Bool raise {
  if @wasmkeep.env("MOTD") is Some(motd) {
    @wasmkeep.send(conn, "* \{motd}") |> ignore
  }
  for line in history() {
    @wasmkeep.send(conn, line) |> ignore
  }
  announce("joined")
  true
}

///|
fn message(_conn : UInt64, msg : @wasmkeep.Message) -> Unit raise {
  if msg is Text(text) {
    say(text)
  }
}

///|
fn is_space(b : Byte) -> Bool {
  b == b' ' || (b >= b'\t' && b <= b'\r')
}

///|
fn fetch(req : @wasmkeep.Request) -> @wasmkeep.Response raise {
  match req.method_ {
    "POST" => {
      let body = req.body()
      let mut start = 0
      let mut end = body.length()
      while start < end && is_space(body[start]) {
        start += 1
      }
      while end > start && is_space(body[end - 1]) {
        end -= 1
      }
      // Too long or not UTF-8: refused, as an empty body is.
      let text = if body.length() > 4096 {
        ""
      } else {
        @utf8.decode(body[start:end]) catch {
          _ => ""
        }
      }
      if text == "" {
        return @wasmkeep.reply(400, "send a UTF-8 text body\n")
      }
      say(text)
      @wasmkeep.reply(200, "sent\n")
    }
    "GET" => {
      let buf = StringBuilder::new()
      for line in history() {
        buf.write_string(line)
        buf.write_char('\n')
      }
      if buf.is_empty() {
        buf.write_char('\n')
      }
      @wasmkeep.reply(200, buf.to_string())
    }
    _ => @wasmkeep.reply(405, "use GET or POST\n")
  }
}
