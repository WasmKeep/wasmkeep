// The actor's hooks: added to the generated world package by build.sh,
// dispatching to the app's `actor` (package app).

///|
pub fn on_init() -> Result[Unit, String] {
  @wasmkeep.dispatch_init(@app.actor)
}

///|
pub fn on_alarm() -> Result[Unit, String] {
  @wasmkeep.dispatch_alarm(@app.actor)
}

///|
pub fn on_websocket_open(conn : UInt64, path : String) -> Result[Unit, String] {
  @wasmkeep.dispatch_open(@app.actor, conn, path)
}

///|
pub fn on_websocket_message(conn : UInt64, msg : @websockets.Message) -> Unit {
  @wasmkeep.dispatch_message(@app.actor, conn, msg)
}

///|
pub fn on_websocket_close(conn : UInt64, code : UInt, reason : String) -> Unit {
  @wasmkeep.dispatch_close(@app.actor, conn, code, reason)
}
