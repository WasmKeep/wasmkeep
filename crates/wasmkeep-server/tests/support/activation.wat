(component
  (import "wasi:io/poll@0.2.0" (instance $poll
    (export "pollable" (type (sub resource)))
    (export "[method]pollable.block" (func (param "self" (borrow 0))))))
  (alias export $poll "pollable" (type $pollable))
  (import "wasi:clocks/monotonic-clock@0.2.0" (instance $clock
    (alias outer 1 $pollable (type $p))
    (export "subscribe-duration" (func (param "when" u64) (result (own $p))))))
  (import "wasi:http/types@0.2.0" (instance $types
    (export "incoming-request" (type (sub resource)))
    (export "response-outparam" (type (sub resource)))))
  (alias export $types "incoming-request" (type $req))
  (alias export $types "response-outparam" (type $out))
  (core func $sleep (canon lower (func $clock "subscribe-duration")))
  (core func $block (canon lower (func $poll "[method]pollable.block")))
  (core module $m
    (import "" "sleep" (func $sleep (param i64) (result i32)))
    (import "" "block" (func $block (param i32)))
    (func $start i64.const 60000000000 call $sleep call $block)
    (start $start)
    (func (export "handle") (param i32 i32)))
  (core instance $i (instantiate $m (with "" (instance
    (export "sleep" (func $sleep)) (export "block" (func $block))))))
  (func $handle (param "request" (own $req)) (param "response-out" (own $out))
    (canon lift (core func $i "handle")))
  (instance $h (export "handle" (func $handle)))
  (export "wasi:http/incoming-handler@0.2.0" (instance $h)))
