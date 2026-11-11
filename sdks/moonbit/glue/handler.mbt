// The request handler: added to the generated incoming-handler package
// by build.sh, dispatching to the app's `actor` (package app).

///|
pub fn handle(request : @types.IncomingRequest, response_out : @types.ResponseOutparam) -> Unit {
  @wasmkeep.dispatch_fetch(@app.actor, request, response_out)
}
