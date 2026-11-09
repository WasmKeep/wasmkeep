;; Smallest useful test component: one core module with a linear memory and
;; a function lifted to the component export `run`, which returns 42.
(component
  (core module $m
    (memory (export "memory") 1)
    (func (export "run") (result i32) i32.const 42))
  (core instance $i (instantiate $m))
  (func (export "run") (result s32) (canon lift (core func $i "run"))))
