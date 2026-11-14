# Helpers for the tutorial tests, sourced by scripts/test-tutorials.sh before
# each scripts/tutorials/<name>.sh. A test runs the commands of
# docs/tutorials/<name>.md as written: each fenced block that follows a
# `<!-- test: ID -->` comment is run by `run ID`, in this one shell, as a
# reader would paste it into one terminal. The test file adds only the
# checks of what the page says you should see.
#
# A test exits 77 to say it was skipped (a tool it needs is missing).

TUTORIAL=${TUTORIAL:?set TUTORIAL to docs/tutorials/<name>.md}
OUT=
_out_file=$(mktemp)
_err_file=$(mktemp)
# curl prints a progress meter when its output is not a terminal, as here:
# turn it off, so a test sees what a reader sees.
CURL_HOME=$(mktemp -d)
export CURL_HOME
echo no-progress-meter >"$CURL_HOME/.curlrc"
_on_fail=()

# The text of block ID, without its fences.
block() {
  local text
  text=$(awk -v id="$1" '
    $0 == "<!-- test: " id " -->" { want = 1; next }
    want && /^```/ { if (inside) exit; inside = 1; next }
    inside { print }
  ' "$TUTORIAL")
  [ -n "$text" ] || fail "no block <!-- test: $1 --> in $TUTORIAL"
  printf '%s\n' "$text"
}

# Run block ID in this shell; what it prints on stdout is in $OUT (stderr,
# such as what `time` prints, is shown only).
# A command that fails does not stop the test: the checks that follow say
# what was expected.
run() {
  local code
  code=$(block "$1") || exit 1
  printf '\n-- %s\n' "$1"
  printf '%s\n' "$code" | sed 's/^/$ /'
  set +e
  eval "$code" >"$_out_file" 2>"$_err_file"
  RUN_STATUS=$?
  set -e
  OUT=$(tr -d '\r' <"$_out_file")
  [ -z "$OUT" ] || printf '%s\n' "$OUT" | sed 's/^/  > /'
  sed 's/^/  2> /' "$_err_file"
}

# Run block ID until `expect`-style check CHECK (a function name) passes,
# at most SECS seconds, once a second.
run_until() {
  local secs=$1 id=$2 check=$3 start=$SECONDS
  while :; do
    run "$id"
    if "$check" 2>/dev/null; then return 0; fi
    [ $((SECONDS - start)) -lt "$secs" ] || fail "block $id: $check still false after ${secs}s"
    sleep 1
  done
}

expect() {
  printf '%s\n' "$OUT" | grep -Eq -- "$1" || fail "expected a line matching '$1'"
  echo "  ok: $1"
}

expect_not() {
  if printf '%s\n' "$OUT" | grep -Eq -- "$1"; then fail "expected no line matching '$1'"; fi
  echo "  ok: no $1"
}

expect_status() {
  [ "$RUN_STATUS" -eq "$1" ] || fail "expected exit status $1, got $RUN_STATUS"
  echo "  ok: exit status $1"
}

# A function to run before exiting on a failure (to show logs).
on_fail() { _on_fail+=("$1"); }

fail() {
  echo "FAIL ($TUTORIAL): $*" >&2
  local f
  for f in "${_on_fail[@]}"; do "$f" >&2 || true; done
  exit 1
}

skip() {
  echo "skipped ($TUTORIAL): $*"
  exit 77
}

need() {
  local tool
  for tool in "$@"; do
    command -v "$tool" >/dev/null 2>&1 || skip "$tool is not installed"
  done
}

# Fail unless nothing listens on these TCP ports on 127.0.0.1.
ports_free() {
  local port
  for port in "$@"; do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      fail "port $port is in use; stop what listens on it first"
    fi
  done
}
