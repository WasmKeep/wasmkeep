# docs/tutorials/one-node-to-three.md: three nodes against moto, one actor
# through every node, a kill -9 and a failover, a restart, a drain.
# Sourced by scripts/test-tutorials.sh after lib.sh, from the repository's
# root, which stands for the reader's clone (the `clone` block is not run).

need cargo curl awk timeout
if command -v uvx >/dev/null 2>&1; then moto_block=moto-uvx
elif command -v moto_server >/dev/null 2>&1; then moto_block=moto-pip
else skip "no local S3 store: install uv (for uvx) or moto[server]==5.2.5 (moto_server)"
fi

ports_free 9000 8081 8082 8083 7481 7482 7483
rm -rf .wasmkeep/cluster

# Whatever the page started, stopped by the pids it saved (never by name:
# other clusters may run on this machine). SIGTERM, which uvx passes on to
# moto (a SIGKILL would leave moto running); nodes are killed outright.
stop_all() {
  local pid
  for pid in .wasmkeep/cluster/*.pid; do
    [ -f "$pid" ] || continue
    case $pid in
      */moto.pid) kill "$(cat "$pid")" 2>/dev/null || true ;;
      *) kill -9 "$(cat "$pid")" 2>/dev/null || true ;;
    esac
  done
  wait 2>/dev/null || true
}
show_logs() {
  local log
  for log in .wasmkeep/cluster/*.log; do
    [ -f "$log" ] || continue
    echo "--- last lines of $log"; tail -n 40 "$log"
  done
}
trap stop_all EXIT
trap 'exit 143' TERM
on_fail show_logs

# 1. The server (`clone` is this checkout).
run build-server
expect_status 0
expect '^wasmkeep [0-9]'
[ "$(command -v wasmkeep)" = "$PWD/target/release/wasmkeep" ] || fail "wasmkeep is not the one just built"

# 2. The counter. (Checked after the first cargo command, which installs
# the toolchain rust-toolchain.toml names, with its targets.)
rustup target list --installed 2>/dev/null | grep -qx wasm32-wasip2 \
  || skip "the wasm32-wasip2 Rust target is not installed (rustup target add wasm32-wasip2)"
run build-counter
expect_status 0

# 3. The bucket.
run "$moto_block"
run bucket
expect_status 0

# 4. Deploy.
run deploy
expect_status 0
expect '^  class counter: added (open)$'
expect '^demo/shop: version [0-9a-f]{16} is current \(deploy 1\)$'

# 5. Three nodes.
run start-nodes
run wait-ready
expect '^8081 ready$'
expect '^8083 ready$'

# 6. One actor through every node.
run first-post
expect '^HTTP/1.1 200 OK'
expect '^x-wasmkeep-node: a'
expect '^1'
run other-nodes
expect_status 0
[ "$OUT" = "$(printf '2\n3\nx-wasmkeep-node: a')" ] \
  || fail "expected 2, 3 and x-wasmkeep-node: a"
echo "  ok: 2, 3, x-wasmkeep-node: a"

# 7. Status: three live nodes, a holds the actor (load lags a heartbeat).
status_shows_alice_on_a() {
  printf '%s\n' "$OUT" | grep -Eq '^a +yes +[0-9.]+m?s +serving +1 '
}
run_until 15 status status_shows_alice_on_a
expect_status 0
for node in a b c; do expect "^$node +yes +[0-9.]+m?s +serving "; done
expect '^NODE +LIVE +AGE +STATE +ACTORS +SOCKETS +EV/MIN +FRONT/MIN +VERSION +APP +ADDRESS +CLUSTER ADDRESS +TLS$'
expect '^APP +DEPLOYED VERSION +SEQ +ACTORS +FRONT$'
expect '^demo/shop +[0-9a-f]{16} +1 +1 +no$'
expect '^3 of 3 nodes live; TLS: 3 with a cluster listener, 0 plain'

# 8. kill -9 a, right after a write.
run kill
[ "$(printf '%s\n' "$OUT" | head -n 1)" = 4 ] || fail "expected 4 before the kill"
echo "  ok: 4"
run post-after-kill
expect '^HTTP/1.1 503 Service Unavailable'
expect '^retry-after: 1'
expect "^the actor's node did not answer; retry"
started=$(date +%s%N)
run get-after-kill
waited_ms=$(( ($(date +%s%N) - started) / 1000000 ))
expect '^x-wasmkeep-node: [bc]'
expect '^4'
# The page: taken over 15 to about 21 s after a's last heartbeat (written
# before the kill), plus a restore. Past the 25 s a GET is kept
# (--failover-wait-secs), it would have been a 503.
[ "$waited_ms" -le 25000 ] || fail "the GET waited ${waited_ms} ms, more than the 25 s it is kept"
echo "  ok: answered after ${waited_ms} ms"
run post-after-failover
expect '^5$'
# Not live: STATE (and the load) is `-`, whatever the last heartbeat said.
expect '^a +no \(fenced\) +[0-9.]+(m?s|m) +- +- +- +- +- '
expect '^2 of 3 nodes live'

# 9. Restart a.
run restart-a
expect_status 0
run after-restart
[ "$(printf '%s\n' "$OUT" | sed -n '1,2p')" = "$(printf 'x-wasmkeep-node: a\n1')" ] \
  || fail "expected bob on a with 1"
echo "  ok: bob on a, 1"
expect '^x-wasmkeep-node: [bc]'
expect '^5'

# 10. Drain a.
run drain
expect_status 0
expect '^Asked a to drain'
run wait-drained
expect_status 0
expect '^a +yes +[0-9.]+m?s +drained '
run after-drain
expect '^x-wasmkeep-node: [bc]'
expect '^1'
run stop-drained
expect_status 0
status_shows_a_released() {
  printf '%s\n' "$OUT" | grep -Eq '^a +no \(released\) '
}
run_until 30 status-after-stop status_shows_a_released
expect '^a +no \(released\) +[0-9.]+(m?s|m) +- +- +- +- +- '

# 11. Clean up.
run cleanup
expect_status 0
[ ! -e .wasmkeep/cluster ] || fail ".wasmkeep/cluster is still there"
ports_free 9000 8081 8082 8083
echo "  ok: cleaned up"
