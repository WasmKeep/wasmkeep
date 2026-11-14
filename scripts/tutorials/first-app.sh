# docs/tutorials/first-app.md: the todo-front example copied, `wasmkeep
# dev` with a token from .env.local (the page, 401, 201/200, 403 on
# /actor/), then a deploy to moto, one node, the front's 503 until its
# secret is set, a change of the front redeployed and rolled back.
# Sourced by scripts/test-tutorials.sh after lib.sh, from the repository's
# root, which stands for the reader's clone (the `clone` block is not run).
#
# Not run: signing in from a browser (the page's script is not executed;
# `curl` reads the page and drives the same API).

need cargo curl awk timeout openssl sed
if command -v uvx >/dev/null 2>&1; then moto_block=moto-uvx
elif command -v moto_server >/dev/null 2>&1; then moto_block=moto-pip
else skip "no local S3 store: install uv (for uvx) or moto[server]==5.2.5 (moto_server)"
fi

ports_free 8080 9000
rm -rf .wasmkeep/todo
root=$PWD
# The page changes directory (into .wasmkeep/todo).
TUTORIAL=$root/$TUTORIAL
app=$root/.wasmkeep/todo

# Whatever the page started, stopped by the pids it saved (never by name).
# SIGTERM, which uvx passes on to moto; dev and the node are killed outright.
stop_all() {
  local pid
  for pid in "$app"/*.pid; do
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
  for log in "$app"/*.log; do
    [ -f "$log" ] || continue
    echo "--- last lines of $log"; tail -n 40 "$log"
  done
}
trap stop_all EXIT
trap 'exit 143' TERM
on_fail show_logs

# 1. The server.
run build-server
expect_status 0
expect '^wasmkeep [0-9]'
[ "$(command -v wasmkeep)" = "$root/target/release/wasmkeep" ] || fail "wasmkeep is not the one just built"
rustup target list --installed 2>/dev/null | grep -qx wasm32-wasip2 \
  || skip "the wasm32-wasip2 Rust target is not installed (rustup target add wasm32-wasip2)"

# 2. The copy.
run copy
expect_status 0
[ "$PWD" = "$app" ] || fail "the copy did not cd into .wasmkeep/todo"
cmp -s wasmkeep.toml "$root/examples/todo-front/wasmkeep.toml" || fail "the copy's wasmkeep.toml differs"
# The file the page shows is the example's, without its comments.
shown=$(awk '/^## 3\./ { s = 1 } s && /^```toml$/ { i = 1; next } i && /^```$/ { exit } i' "$TUTORIAL")
[ "$(printf '%s\n' "$shown" | grep -v '^$')" = "$(grep -v -e '^#' -e '^$' wasmkeep.toml)" ] \
  || fail "the page's wasmkeep.toml is not the example's without its comments"
echo "  ok: the page shows the example's wasmkeep.toml"

# 4. wasmkeep dev.
run env-local
expect_status 0
grep -Eq '^TOKENS=alice:[0-9a-f]{64}$' .env.local || fail ".env.local has no alice token"
[ "$(stat -c %a .env.local)" = 600 ] || fail ".env.local is not 0600"
run dev
dev_log=$(sed 's/\x1b\[[0-9;]*m//g' dev.log)
for line in 'running the project file app=acme/todo file=wasmkeep.toml' \
  'serving the front at http://127.0.0.1:8080/' \
  'serving todos at http://127.0.0.1:8080/actor/todos/<name>/ \(access internal\)'; do
  printf '%s\n' "$dev_log" | grep -Eq "$line" || fail "dev.log has no '$line'"
  echo "  ok: dev.log: $line"
done
if printf '%s\n' "$dev_log" | grep -q 'readable by every user'; then fail "dev warned about .env.local"; fi
run page
expect '^<h1>Todos</h1>$'

# 5. The API.
run no-token
expect '^HTTP/1.1 401 Unauthorized'
expect '^www-authenticate: Bearer'
expect '^sign in with Authorization: Bearer <token>$'
run with-token
[ "$OUT" = "$(printf '{"id":1}\n201\n[{"id":1,"text":"buy milk"}]\n200')" ] \
  || fail "expected {\"id\":1} 201, then the list and 200"
echo "  ok: 201, then 200 with the list"

# 6. Internal.
run actor-outside
expect '^HTTP/1.1 403 Forbidden'
expect '^not reachable from outside$'
grep -qF 'if caller(&request) != Caller::Front {' todos/src/lib.rs || fail "the caller() check the page quotes is not in todos"
grep -qF 'match actors::call(&format!("{CLASS}/{user}"), &call) {' front/src/lib.rs || fail "the call the page quotes is not in the front"
run stop-dev
ports_free 8080
echo "  ok: dev stopped"

# 7. The bucket.
run "$moto_block"
run bucket
expect_status 0

# 8. Deploy.
run key
expect_status 0
[ -f secrets.key ] || fail "no secrets.key"
run deploy
expect_status 0
expect '^acme/todo from wasmkeep.toml, before switching:$'
expect '^  front: added$'
expect '^  class todos: added$'
expect '^  requires _front: TOKENS now required$'
expect '^acme/todo: version [0-9a-f]{16} is current \(deploy 1\)$'
v1=$(printf '%s\n' "$OUT" | sed -n 's/^acme\/todo: version \([0-9a-f]*\) is current.*/\1/p')

# 9. One node; the front answers 503 until TOKENS is set.
run serve
run front-503
expect '^HTTP/1.1 503 Service Unavailable'
expect '^retry-after: 1'
expect '^this node cannot serve the app now; retry$'
run front-log
expect '^1$'

# 10. The front's secret.
run secret
expect_status 0
expect '^Set 1 secret\(s\) for acme/todo: seq 1\.$'
run use-node
[ "$OUT" = "$(printf '{"id":1}\n201\nnot reachable from outside\n403')" ] \
  || fail "expected {\"id\":1} 201, then the 403 of /actor/"
echo "  ok: 201 through the front, 403 on /actor/"

# 11. A change of the front.
run change
expect_status 0
expect '^  front: changed$'
expect_not 'class todos'
expect '^acme/todo: version [0-9a-f]{16} is current \(deploy 2\)$'
expect_not "version $v1 "
run after-change
[ "$OUT" = "$(printf '<h1>Our todos</h1>\n[{"id":1,"text":"ship it"}]')" ] \
  || fail "expected the new heading and the list kept"
echo "  ok: the new heading, the list kept"

# 12. Rollback to the first version.
run rollback
expect_status 0
expect '^  front: changed$'
expect "^acme/todo: version $v1 is current \(deploy 3\)$"
run after-rollback
expect '^<h1>Todos</h1>$'
grep -q '<h1>Our todos</h1>' front/src/page.html || fail "the page says the file keeps the change"

# 13. Clean up.
run cleanup
expect_status 0
[ "$PWD" = "$root" ] || fail "the cleanup did not return to the clone"
[ ! -e .wasmkeep/todo ] || fail ".wasmkeep/todo is still there"
ports_free 8080 9000
echo "  ok: cleaned up"
