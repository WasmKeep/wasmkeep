# docs/tutorials/going-to-production.md: the commands of the checklist that
# run against a cluster, on the page's practice cluster (three nodes against
# moto): cluster diagnose, the TLS pins in cluster status, secrets set
# (a class's and the front's) and list, a deploy with --requires and its
# rollback, the metrics page, a drain and an undrain.
# Sourced by scripts/test-tutorials.sh after lib.sh, from the repository's
# root, which stands for the reader's clone.
#
# Not run, and why:
# - Steps 1 to 3 of one-node-to-three.md (server, counter, bucket), which
#   the page asks for first, are run from that page's blocks (its deploy
#   and nodes are not: this page has its own).
# - The production values of BUCKET and APP, the curl through a load
#   balancer and the nginx lines: they need a real domain, a certificate
#   and a proxy.
# - The systemd settings, the firewall, the alert rules (promtool) and the
#   Grafana import: they need the real machines and a Prometheus.
# - Backups, the restore drill and upgrades: they stop a real cluster and
#   copy a real bucket with tools of the reader's choice.
# - Stopping the drained node with systemctl: the practice nodes are not
#   services; the page's undrain block runs on the drained node instead.

need cargo curl awk timeout
if command -v uvx >/dev/null 2>&1; then moto_block=moto-uvx
elif command -v moto_server >/dev/null 2>&1; then moto_block=moto-pip
else skip "no local S3 store: install uv (for uvx) or moto[server]==5.2.5 (moto_server)"
fi

ports_free 9000 8081 8082 8083 7481 7482 7483 9100 9101 9102
rm -rf .wasmkeep/cluster .wasmkeep/prod
root=$PWD
# The page changes directory (into .wasmkeep/prod), so name both pages by
# their absolute paths.
TUTORIAL=$root/$TUTORIAL
previous=$root/docs/tutorials/one-node-to-three.md

# Whatever the pages started, stopped by the pids they saved (never by
# name: other clusters may run on this machine). moto gets SIGTERM, which
# uvx passes on; nodes are killed outright.
stop_all() {
  local pid
  for pid in "$root"/.wasmkeep/cluster/moto.pid "$root"/.wasmkeep/prod/*.pid; do
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
  for log in "$root"/.wasmkeep/cluster/*.log "$root"/.wasmkeep/prod/*.log; do
    [ -f "$log" ] || continue
    echo "--- last lines of $log"; tail -n 40 "$log"
  done
}
trap stop_all EXIT
trap 'exit 143' TERM
on_fail show_logs

# The previous tutorial's steps 1 to 3.
TUTORIAL=$previous run build-server
expect_status 0
[ "$(command -v wasmkeep)" = "$root/target/release/wasmkeep" ] || fail "wasmkeep is not the one just built"
rustup target list --installed 2>/dev/null | grep -qx wasm32-wasip2 \
  || skip "the wasm32-wasip2 Rust target is not installed (rustup target add wasm32-wasip2)"
TUTORIAL=$previous run build-counter
expect_status 0
TUTORIAL=$previous run "$moto_block"
TUTORIAL=$previous run bucket
expect_status 0

# 0. The practice cluster.
run practice-setup
expect_status 0
[ "$PWD" = "$root/.wasmkeep/prod" ] || fail "the setup did not cd into .wasmkeep/prod"
expect '^demo/shop: version [0-9a-f]{16} is current \(deploy 1\)$'
[ -f secrets.key ] || fail "no secrets.key"
v1=$(printf '%s\n' "$OUT" | sed -n 's/^demo\/shop: version \([0-9a-f]*\) is current.*/\1/p')
run practice-nodes
expect '^1$'

# 1. The bucket.
run diagnose
expect_status 0
expect '^pass +create-if-absent '
expect '^pass +replace-if-match '
expect '^safe for WasmKeep$'

# 3. Pins. (ACTORS lags a heartbeat, so it is not checked.)
run status
expect_status 0
for node in a b c; do expect "^$node +yes +.* https://127\.0\.0\.1:748[123] +pinned$"; done
expect '^3 of 3 nodes live; TLS: 3 with a cluster listener, 0 plain'
expect_not '^warning:'

# 5. Secrets.
run secret-files
expect_status 0
run secrets-set
expect_status 0
expect '^Set 1 secret\(s\) for demo/shop: seq 1\.$'
expect '^Set 1 secret\(s\) for demo/shop: seq 2\.$'
expect '^demo/shop: seq 2$'
expect '^  _front \(the front\): SESSION_KEY$'
expect '^  counter: ADMIN_TOKEN$'
expect_not 'practice-'

# 6. Deploy and rollback. The required value is set, so the class still
# answers once the nodes follow the new version.
run deploy
expect_status 0
expect '^  requires counter: ADMIN_TOKEN now required$'
expect '^demo/shop: version [0-9a-f]{16} is current \(deploy 2\)$'
sleep 4
[ "$(curl -s -X POST localhost:8082/actor/counter/alice)" = 2 ] || fail "the counter did not answer 2 after the deploy"
echo "  ok: the counter answers under the version that requires ADMIN_TOKEN"
run rollback
expect_status 0
expect '^  requires counter: ADMIN_TOKEN no longer required$'
expect "^rolled back demo/shop: [0-9a-f]{16} -> $v1 \\(deploy 3\\)$"
expect_not 'is current'


# 7. Metrics: the series the page names are on node a's page.
run metrics
expect_status 0
expect '^wasmkeep_ready 1$'
expect '^wasmkeep_bucket_requests_total\{operation="get",status="ok"\} [0-9]+$'
# On the page from the start, before any bucket error.
expect '^wasmkeep_bucket_requests_total\{operation="get",status="error"\} [0-9]+$'
for reason in access capacity overloaded unavailable wrong_app; do
  expect "^wasmkeep_admission_refused_total\{reason=\"$reason\"\} [0-9]+$"
done

# 10. Drain a, then undrain it.
run drain
expect_status 0
expect '^Asked a to drain'
# The page: a drained node still reports ready.
[ "$(curl -s localhost:8081/_wasmkeep/ready)" = ready ] || fail "drained a does not report ready"
echo "  ok: drained a reports ready"
run undrain
expect_status 0
expect '^Withdrew the drain request for a\.$'
status_shows_a_serving() {
  printf '%s\n' "$OUT" | grep -Eq '^a +yes +[0-9.]+m?s +serving '
}
run_until 30 status status_shows_a_serving

# 11. Clean up.
run cleanup
expect_status 0
[ "$PWD" = "$root" ] || fail "the cleanup did not return to the clone"
[ ! -e .wasmkeep/prod ] && [ ! -e .wasmkeep/cluster ] || fail ".wasmkeep/prod or .wasmkeep/cluster is still there"
ports_free 9000 8081 8082 8083 9100
echo "  ok: cleaned up"
