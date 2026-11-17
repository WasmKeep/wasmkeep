# docs/build/react.md: the React app written from an empty directory (each
# file the page has you write is compared with examples/react-todos), built,
# run with `wasmkeep dev` and Vite side by side, deployed to moto with its
# assets, and served by one node.
# Sourced by scripts/test-tutorials.sh after lib.sh, from the repository's
# root, which stands for the reader's machine.
#
# Differences from a reader's run: `wasmkeep` is the one built from the
# clone, and `@wasmkeep/sdk@<version>` (the release the page names, 0.6.0) is
# replaced by the clone's SDK, as `npm install` of a path does (run `npm ci`
# in sdks/typescript first). Not run: the page in a browser (curl asks for
# the same files and the same API).

need cargo curl awk timeout sed node npm
node -e 'const [a, b] = process.versions.node.split(".").map(Number); process.exit(a > 22 || (a === 22 && b >= 12) ? 0 : 1)' \
  || skip "Node.js 22.12 or later is needed"
if command -v uvx >/dev/null 2>&1; then moto_block=moto-uvx
elif command -v moto_server >/dev/null 2>&1; then moto_block=moto-pip
else skip "no local S3 store: install uv (for uvx) or moto[server]==5.2.5 (moto_server)"
fi
root=$PWD
sdk=$root/sdks/typescript
[ -d "$sdk/node_modules" ] || skip "run npm ci in sdks/typescript first"
example=$root/examples/react-todos

ports_free 8080 9000 5173
work=$root/.wasmkeep/react-tutorial
rm -rf "$work"
mkdir -p "$work"
cd "$work"
# The page changes directory (into react-todos).
TUTORIAL=$root/$TUTORIAL
app=$work/react-todos

# The page installs the SDK from npm; here it comes from the clone.
npm() {
  local args=() arg
  for arg in "$@"; do args+=("${arg/@wasmkeep\/sdk@[0-9]*.[0-9]*.[0-9]*/file:$sdk}"); done
  command npm "${args[@]}"
}

# Whatever the page started, stopped by the pids it saved (never by name).
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

# The file the page wrote is the example's.
same() {
  cmp -s "$1" "$example/$1" || fail "$1 is not the example's"
  echo "  ok: $1 is the example's"
}

# 1. The server, from the clone.
(cd "$root" && cargo build --release -p wasmkeep-cli) >&2
export PATH="$root/target/release:$PATH"
run tools
expect_status 0
expect '^wasmkeep [0-9]'

# 2. The project.
run mkdir
expect_status 0
[ "$PWD" = "$app" ] || fail "the page did not cd into react-todos"
run package-json
expect_status 0
run install
expect_status 0
node -e '
  const read = (f) => JSON.parse(require("fs").readFileSync(f, "utf8"));
  const page = read("package.json"), example = read(process.argv[1]);
  const rest = (o) => { const c = { ...o }; delete c["@wasmkeep/sdk"]; return c; };
  const same = (a, b) => JSON.stringify(Object.entries(a).sort()) === JSON.stringify(Object.entries(b).sort());
  // The example also has `test`, which the page does not write.
  const { test, ...scripts } = example.scripts;
  if (!test || !same(page.scripts, scripts)) throw new Error("scripts differ");
  if (!same(rest(page.dependencies), rest(example.dependencies))) throw new Error("dependencies differ");
  if (!same(page.devDependencies, example.devDependencies)) throw new Error("devDependencies differ");
  for (const d of [page.dependencies, page.devDependencies]) {
    for (const [name, v] of Object.entries(d)) if (name !== "@wasmkeep/sdk" && !/^[0-9]+\.[0-9]+\.[0-9]+$/.test(v)) throw new Error(name + " is not pinned: " + v);
  }
' "$example/package.json" || fail "package.json is not the example's (scripts, pinned dependencies)"
echo "  ok: package.json has the example's scripts and pinned versions"
[ -f package-lock.json ] || fail "npm wrote no package-lock.json"
for id in gitignore:.gitignore tsconfig:tsconfig.json web-tsconfig:web/tsconfig.json; do
  run "${id%%:*}"
  same "${id#*:}"
done

# 3 to 5. The contract, the actor, the page.
run contract
same src/contract.ts
run actor
same src/todos.ts
run build-actor
expect_status 0
[ -s todos.wasm ] || fail "no todos.wasm"
for id in html:web/index.html vite-config:web/vite.config.ts api:web/src/api.ts app:web/src/App.tsx \
  main:web/src/main.tsx styles:web/src/styles.css sdk-types:web/src/sdk-types.d.ts; do
  run "${id%%:*}"
  same "${id#*:}"
done
run typecheck
expect_status 0

# 6. The project file: the example's, without its comments.
run project-file
expect_status 0
[ "$(grep -v -e '^#' -e '^$' wasmkeep.toml)" = "$(grep -v -e '^#' -e '^$' "$example/wasmkeep.toml")" ] \
  || fail "wasmkeep.toml is not the example's without its comments"
echo "  ok: wasmkeep.toml is the example's"

# 7. dev and Vite.
run dev
dev_log=$(sed 's/\x1b\[[0-9;]*m//g' dev.log)
for line in 'running the project file app=acme/react-todos file=wasmkeep.toml' \
  'serving todos at http://127.0.0.1:8080/actor/todos/<name>/ \(access open\)'; do
  printf '%s\n' "$dev_log" | grep -Eq "$line" || fail "dev.log has no '$line'"
  echo "  ok: dev.log: $line"
done
run pages
[ "$OUT" = "$(printf '1\n1\n1\n{"id":1,"text":"buy milk","done":false}\n201\n{"todos":[{"id":1,"text":"buy milk","done":false}]}')" ] \
  || fail "pages: not the output the page shows"
echo "  ok: the page at 5173 and 8080, the actor through the proxy"
run spa
[ "$OUT" = 404 ] || fail "a missing script is not a 404"
echo "  ok: a missing script is a 404"
run error
[ "$OUT" = "$(printf '{"v":1,"code":"todos.invalid_text","message":"Write 1 to 200 characters","details":{"max":200}}\n400')" ] \
  || fail "error: not the output the page shows"
echo "  ok: the typed error"
run stop-dev
ports_free 8080 5173
echo "  ok: dev and Vite stopped"

# 8. The bucket.
run "$moto_block"
run bucket
expect_status 0

# 9. Deploy and a node.
run deploy
expect_status 0
expect '^acme/react-todos from wasmkeep.toml, before switching:$'
expect '^  class todos: added \(open\)$'
expect '^  asset assets/index-[A-Za-z0-9_-]+\.js: [0-9]+ bytes, text/javascript;charset=utf-8$'
expect '^  asset assets/index-[A-Za-z0-9_-]+\.css: [0-9]+ bytes, text/css;charset=utf-8$'
expect '^  asset index.html: [0-9]+ bytes, text/html;charset=utf-8$'
expect '^  assets: added$'
expect '^acme/react-todos: version [0-9a-f]{16} is current \(deploy 1\)$'
run serve
run node-page
[ "$OUT" = "$(printf '1\ncache-control: public, max-age=0, must-revalidate\ncache-control: public, max-age=31536000, immutable\n{"id":1,"text":"deployed","done":false}\n201')" ] \
  || fail "node-page: not the output the page shows"
echo "  ok: the node serves the page, hashed files kept, the actor answers"

# 10. Clean up.
run cleanup
expect_status 0
[ "$PWD" = "$work" ] || fail "the cleanup did not leave react-todos"
[ ! -e react-todos ] || fail "react-todos is still there"
ports_free 8080 9000 5173
cd "$root"
rm -rf "$work"
echo "  ok: cleaned up"
