// WasmKeep actors in MoonBit, over the bindings `wit-bindgen moonbit`
// generates (see ../build.sh). An app is a package that defines its hooks
// as an `Actor`:
//
//   pub let actor : @wasmkeep.Actor = @wasmkeep.Actor::new(
//     init=fn() { @wasmkeep.exec("CREATE TABLE IF NOT EXISTS hits (at INTEGER)") |> ignore },
//     fetch=fn(req) { @wasmkeep.reply(200, "hello from \{req.path}\n") },
//   )
//
// The host delivers one event at a time to an actor, in arrival order, so
// nothing here needs locks. Failures are raised as `Failure` (the builtin
// error `fail` raises): from `init` or `alarm` they fail the event (an
// alarm is retried).

// ------------------------------------------------------------------ hooks

///|
/// An actor's hooks, each optional.
pub struct Actor {
  priv init : (() -> Unit raise)?
  priv fetch : ((Request) -> Response raise)?
  priv alarm : (() -> Unit raise)?
  priv open : ((UInt64, String) -> Bool raise)?
  priv message : ((UInt64, Message) -> Unit raise)?
  priv close : ((UInt64, Int, String) -> Unit raise)?
}

///|
/// The hooks: `init` runs before the first event an instance handles with
/// this component version (create or migrate tables); `fetch` answers
/// HTTP requests and calls from other actors (404 without it); `alarm`
/// runs when the alarm is due; `open` accepts (true) or refuses a
/// WebSocket (without it, connections are accepted when `message` is
/// given); `message` and `close` follow the connection.
pub fn Actor::new(
  init? : () -> Unit raise,
  fetch? : (Request) -> Response raise,
  alarm? : () -> Unit raise,
  open? : (UInt64, String) -> Bool raise,
  message? : (UInt64, Message) -> Unit raise,
  close? : (UInt64, Int, String) -> Unit raise,
) -> Actor {
  { init, fetch, alarm, open, message, close }
}

///|
/// A WebSocket message.
pub(all) enum Message {
  Text(String)
  Binary(Bytes)
}

///|
fn why(e : Error) -> String {
  match e {
    Failure(msg) => msg
    e => e.to_string()
  }
}

///|
#doc(hidden)
pub fn dispatch_init(a : Actor) -> Result[Unit, String] {
  match a.init {
    None => Ok(())
    Some(f) =>
      try f() catch {
        e => Err(why(e))
      } noraise {
        _ => Ok(())
      }
  }
}

///|
#doc(hidden)
pub fn dispatch_alarm(a : Actor) -> Result[Unit, String] {
  match a.alarm {
    None => Ok(())
    Some(f) =>
      try f() catch {
        e => Err(why(e))
      } noraise {
        _ => Ok(())
      }
  }
}

///|
#doc(hidden)
pub fn dispatch_open(a : Actor, conn : UInt64, path : String) -> Result[Unit, String] {
  match a.open {
    None => if a.message is Some(_) { Ok(()) } else { Err("this actor takes no WebSockets") }
    Some(f) =>
      try f(conn, path) catch {
        e => Err(why(e))
      } noraise {
        true => Ok(())
        false => Err("refused")
      }
  }
}

///|
#doc(hidden)
pub fn dispatch_message(a : Actor, conn : UInt64, msg : @websockets.Message) -> Unit {
  guard a.message is Some(f) else { return }
  let m = match msg {
    Text(s) => Message::Text(s)
    Binary(b) => Message::Binary(Bytes::from_array(b[:]))
  }
  // A failure traps the event, as an uncaught error does in the other SDKs.
  f(conn, m) catch {
    e => abort(why(e))
  }
}

///|
#doc(hidden)
pub fn dispatch_close(a : Actor, conn : UInt64, code : UInt, reason : String) -> Unit {
  guard a.close is Some(f) else { return }
  f(conn, code.reinterpret_as_int(), reason) catch {
    e => abort(why(e))
  }
}

///|
#doc(hidden)
pub fn dispatch_fetch(a : Actor, handle : @http.IncomingRequest, out : @http.ResponseOutparam) -> Unit {
  let req = incoming(handle)
  let resp = match a.fetch {
    None => reply(404, "no fetch handler\n")
    Some(f) =>
      f(req) catch {
        e => reply(500, why(e) + "\n")
      }
  }
  handle.drop()
  respond(out, resp)
}

// ----------------------------------------------------------------- storage

///|
/// A statement parameter or a value read back.
pub using @types {type Value}

///|
/// NULL.
pub fn null() -> Value {
  Null
}

///|
/// An INTEGER.
pub fn int(n : Int64) -> Value {
  Integer(n)
}

///|
/// A REAL.
pub fn real(d : Double) -> Value {
  Real(d)
}

///|
/// TEXT.
pub fn text(s : String) -> Value {
  Text(s)
}

///|
/// A BLOB.
pub fn blob(b : Bytes) -> Value {
  Blob(b.to_fixedarray())
}

///|
fn kind_name(k : @types.SqlErrorKind) -> String {
  match k {
    SYNTAX => "syntax"
    CONSTRAINT => "constraint"
    BIND => "bind"
    MULTIPLE_STATEMENTS => "multiple-statements"
    QUOTA_EXCEEDED => "quota-exceeded"
    NOT_ALLOWED => "not-allowed"
    INTERNAL => "internal"
  }
}

///|
fn sql_failure(e : @types.SqlError) -> Error {
  Failure(kind_name(e.kind) + ": " + e.message)
}

///|
/// Runs one statement that returns no rows; the rows changed. Committed to
/// the local database. A failed statement raises `Failure("<kind>: <message>")`.
pub fn exec(sql : String, params? : Array[Value] = []) -> Int64 raise {
  match @storage.exec(sql, params) {
    Ok(n) => n.reinterpret_as_int64()
    Err(e) => raise sql_failure(e)
  }
}

///|
/// `exec`, returning only once the write is durable: replicated to the
/// bucket when the node has one, fsynced locally when it does not. Inside
/// a transaction it takes only COMMIT (or END).
pub fn exec_durable(sql : String, params? : Array[Value] = []) -> Int64 raise {
  match @storage.exec_durable(sql, params) {
    Ok(n) => n.reinterpret_as_int64()
    Err(e) => raise sql_failure(e)
  }
}

///|
/// A query's result: column names, then one array of values per row.
pub struct Rows {
  columns : Array[String]
  values : Array[Array[Value]]
}

///|
/// Runs one statement and returns its rows.
pub fn query(sql : String, params? : Array[Value] = []) -> Rows raise {
  match @storage.query(sql, params) {
    Ok(r) => { columns: r.columns, values: r.values }
    Err(e) => raise sql_failure(e)
  }
}

///|
/// The number of rows.
pub fn Rows::count(self : Rows) -> Int {
  self.values.length()
}

///|
/// The index of the column name, if there is one.
pub fn Rows::column(self : Rows, name : String) -> Int? {
  for i, c in self.columns {
    if c == name {
      return Some(i)
    }
  }
  None
}

///|
/// The value at row, col (NULL past the end).
pub fn Rows::get(self : Rows, row : Int, col : Int) -> Value {
  if row < 0 || row >= self.values.length() {
    return Null
  }
  let r = self.values[row]
  if col < 0 || col >= r.length() {
    Null
  } else {
    r[col]
  }
}

///|
/// The value at row, col as an integer: a REAL truncated, anything else 0.
pub fn Rows::get_int(self : Rows, row : Int, col : Int) -> Int64 {
  match self.get(row, col) {
    Integer(n) => n
    Real(d) => d.to_int64()
    _ => 0
  }
}

///|
/// The value at row, col as a double: an INTEGER converted, anything else 0.
pub fn Rows::get_real(self : Rows, row : Int, col : Int) -> Double {
  match self.get(row, col) {
    Real(d) => d
    Integer(n) => n.to_double()
    _ => 0
  }
}

///|
/// The value at row, col as text: a BLOB decoded as UTF-8, NULL and
/// numbers as "".
pub fn Rows::get_text(self : Rows, row : Int, col : Int) -> String {
  match self.get(row, col) {
    Text(s) => s
    Blob(b) => @utf8.decode_lossy(Bytes::from_array(b[:]))
    _ => ""
  }
}

///|
/// The value at row, col as bytes: TEXT encoded as UTF-8, anything else
/// empty.
pub fn Rows::get_blob(self : Rows, row : Int, col : Int) -> Bytes {
  match self.get(row, col) {
    Blob(b) => Bytes::from_array(b[:])
    Text(s) => @utf8.encode(s)
    _ => b""
  }
}

///|
/// Whether the value at row, col is NULL (or past the end).
pub fn Rows::is_null(self : Rows, row : Int, col : Int) -> Bool {
  self.get(row, col) is Null
}

///|
/// The first column of the first row as an integer; a query with no rows
/// raises.
pub fn query_int(sql : String, params? : Array[Value] = []) -> Int64 raise {
  let rows = query(sql, params~)
  if rows.count() == 0 {
    fail("query returned no rows")
  }
  rows.get_int(0, 0)
}

///|
/// Deletes this actor once the current event ends: its database is
/// removed and its alarm cleared.
pub fn delete_self() -> Unit {
  @storage.delete_self()
}

// ------------------------------------------------------------------ alarms

///|
/// Schedules `alarm` at `at` (milliseconds since the Unix epoch),
/// replacing any pending alarm. Alarms fire at least once.
pub fn set_alarm(at : Int64) -> Unit {
  @alarms.set_alarm((if at < 0 { 0L } else { at }).reinterpret_as_uint64())
}

///|
/// The pending alarm, if any.
pub fn get_alarm() -> Int64? {
  match @alarms.get_alarm() {
    Some(at) => Some(at.reinterpret_as_int64())
    None => None
  }
}

///|
/// Cancels the pending alarm, if any.
pub fn clear_alarm() -> Unit {
  @alarms.clear_alarm()
}

///|
/// Milliseconds since the Unix epoch, from the host's wall clock.
pub fn now_ms() -> Int64 {
  let t = @clock.now()
  t.seconds.reinterpret_as_int64() * 1000L + (t.nanoseconds / 1_000_000).to_int64()
}

// --------------------------------------------------------------------- env

///|
/// The class's configuration value or secret `name`, if it is set.
pub fn env(name : String) -> String? {
  @env.get(name)
}

// ------------------------------------------------------------------ actors

///|
/// An actor's address; "\{class}/\{name}" is what `call` takes.
pub(all) struct Address {
  tenant : String
  class : String
  name : String
}

///|
/// This actor's own address.
pub fn self_address() -> Address {
  let a = @actors.self_address()
  { tenant: a.tenant, class: a.class, name: a.name }
}

///|
/// Another actor's answer.
pub(all) struct CallResponse {
  status : Int
  headers : Array[(String, String)]
  body : Bytes
}

///|
/// Calls the actor at `address` (`class/name`, same tenant) and waits for
/// its answer; other events to this actor wait meanwhile. `verb` is the
/// HTTP method (`method` is reserved in MoonBit). A call that got no
/// answer raises `Failure("<case>: <detail>")`.
pub fn call(
  address : String,
  verb~ : String = "GET",
  path~ : String = "/",
  headers~ : Array[(String, String)] = [],
  body~ : Bytes = b"",
) -> CallResponse raise {
  let req : @actors.Request = { method_: verb, path, headers, body: body.to_fixedarray() }
  match @actors.call(address, req) {
    Ok(r) => { status: r.status.reinterpret_as_int(), headers: r.headers, body: Bytes::from_array(r.body[:]) }
    Err(InvalidAddress(s)) => fail("invalid-address: " + s)
    Err(Reentrant) => fail("reentrant: the call would re-enter an actor in this chain")
    Err(TooLarge) => fail("too-large: body too large")
    Err(Timeout) => fail("timeout: the callee did not answer in time")
    Err(Failed(s)) => fail("failed: " + s)
  }
}

// -------------------------------------------------------------- websockets

///|
/// Queues a text message on `conn`; false if the connection is gone or the
/// client is not reading fast enough (the message was dropped).
pub fn send(conn : UInt64, text : String) -> Bool {
  @websockets.send(conn, Text(text)) is Ok(_)
}

///|
/// Queues a binary message on `conn`.
pub fn send_binary(conn : UInt64, data : Bytes) -> Bool {
  @websockets.send(conn, Binary(data.to_fixedarray())) is Ok(_)
}

///|
/// Sends `text` to every open connection of this actor except `except`.
pub fn broadcast(text : String, except? : UInt64) -> Unit {
  for conn in @websockets.connections() {
    if except != Some(conn) {
      send(conn, text) |> ignore
    }
  }
}

///|
/// Closes `conn` with a close code (1000 is normal) and a reason.
pub fn close(conn : UInt64, code~ : Int = 1000, reason~ : String = "") -> Unit {
  @websockets.close(conn, code.reinterpret_as_uint(), reason)
}

///|
/// This actor's open connections, oldest first.
pub fn connections() -> Array[UInt64] {
  let out = []
  for c in @websockets.connections() {
    out.push(c)
  }
  out
}

// -------------------------------------------------------------------- HTTP

///|
/// An incoming request (or a call from another actor). The path is
/// relative to the actor and includes the query.
pub struct Request {
  method_ : String
  path : String
  headers : Array[(String, String)]
  priv handle : @http.IncomingRequest
  priv mut cache : Bytes?
}

///|
/// The first value of header `name` (any case), if there is one.
pub fn Request::header(self : Request, name : String) -> String? {
  let want = name.to_lower()
  for h in self.headers {
    if h.0.to_lower() == want {
      return Some(h.1)
    }
  }
  None
}

///|
/// The whole body, read on first use.
pub fn Request::body(self : Request) -> Bytes {
  match self.cache {
    Some(b) => b
    None => {
      let b = read_body(self.handle)
      self.cache = Some(b)
      b
    }
  }
}

///|
/// The body as text (UTF-8, invalid sequences replaced).
pub fn Request::text(self : Request) -> String {
  @utf8.decode_lossy(self.body())
}

///|
/// What `fetch` answers. Without a content-type header it is
/// `text/plain; charset=utf-8`.
pub(all) struct Response {
  status : Int
  headers : Array[(String, String)]
  body : Bytes
}

///|
/// A response with a text body.
pub fn reply(status : Int, body : String) -> Response {
  { status, headers: [], body: @utf8.encode(body) }
}

///|
/// A response with a body and its content type.
pub fn reply_bytes(status : Int, content_type : String, body : Bytes) -> Response {
  { status, headers: [("content-type", content_type)], body }
}

///|
/// The response with one more header.
pub fn Response::with_header(self : Response, name : String, value : String) -> Response {
  let headers = self.headers.copy()
  headers.push((name, value))
  { ..self, headers, }
}

///|
fn method_name(m : @http.Method) -> String {
  match m {
    Get => "GET"
    Head => "HEAD"
    Post => "POST"
    Put => "PUT"
    Delete => "DELETE"
    Connect => "CONNECT"
    Options => "OPTIONS"
    Trace => "TRACE"
    Patch => "PATCH"
    Other(s) => s
  }
}

///|
fn incoming(handle : @http.IncomingRequest) -> Request {
  let fields = handle.headers()
  let headers = []
  for e in fields.entries() {
    headers.push((e.0, @utf8.decode_lossy(Bytes::from_array(e.1[:]))))
  }
  fields.drop()
  {
    method_: method_name(handle.method_()),
    path: handle.path_with_query().unwrap_or("/"),
    headers,
    handle,
    cache: None,
  }
}

///|
fn read_body(handle : @http.IncomingRequest) -> Bytes {
  guard handle.consume() is Ok(body) else { return b"" }
  guard body.stream() is Ok(stream) else {
    body.drop()
    return b""
  }
  let buf = Buffer()
  while true {
    match stream.blocking_read(65536) {
      Ok(chunk) => buf.write_bytes(Bytes::from_array(chunk[:]))
      Err(_) => break // closed: the body has ended
    }
  }
  stream.drop()
  @http.IncomingBody::finish(body).drop()
  buf.to_bytes()
}

///|
fn respond(out : @http.ResponseOutparam, r : Response) -> Unit {
  let fields = @http.Fields::fields()
  let typed = r.headers.iter().any(fn(h) { h.0.to_lower() == "content-type" })
  if !typed {
    fields.append("content-type", @utf8.encode("text/plain; charset=utf-8").to_fixedarray()) |> ignore
  }
  for h in r.headers {
    fields.append(h.0, @utf8.encode(h.1).to_fixedarray()) |> ignore
  }
  let resp = @http.OutgoingResponse::outgoing_response(fields)
  resp.set_status_code(r.status.reinterpret_as_uint()) |> ignore
  let body = resp.body()
  @http.ResponseOutparam::set(out, Ok(resp))
  guard body is Ok(outgoing) else { return }
  if outgoing.write() is Ok(stream) {
    // blocking-write-and-flush takes at most 4096 bytes at a time.
    let data = r.body
    let mut at = 0
    while at < data.length() {
      let end = if at + 4096 < data.length() { at + 4096 } else { data.length() }
      let chunk = data[at:end].to_owned().to_fixedarray()
      if stream.blocking_write_and_flush(chunk) is Err(_) {
        break
      }
      at = end
    }
    stream.drop()
  }
  @http.OutgoingBody::finish(outgoing, None) |> ignore
}
