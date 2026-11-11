#!/usr/bin/env bash
# Builds a WasmKeep actor from a MoonBit app package (the directory holding
# its .mbt files and moon.pkg, which imports "wasmkeep/actor/wasmkeep"
# @wasmkeep and defines `pub let actor : @wasmkeep.Actor`).
#
#   sdks/moonbit/build.sh APP_DIR OUT.wasm
#
# It generates the bindings with wit-bindgen moonbit into APP_DIR/gen (a
# MoonBit module), adds the SDK (package wasmkeep), the app (package app)
# and the glue that exports the app's hooks, builds the core module with
# `moon build --target wasm`, and makes the component with wasm-tools
# (MoonBit strings are UTF-16). APP_DIR/gen is replaced on every build; a
# gen directory this script did not write is left alone, and the build
# stops.
#   MoonBit (moon and moonc): https://www.moonbitlang.com/download/
#   cargo install wit-bindgen-cli --version 0.63.0 && cargo install wasm-tools
# WASMKEEP_WIT: the WIT directory, by default the repository's
# crates/wasmkeep-wit/wit beside this SDK.
set -euo pipefail
[ $# -eq 2 ] || { echo 'usage: sdks/moonbit/build.sh APP_DIR OUT.wasm' >&2; exit 2; }
sdk="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$sdk/../.." && pwd)"
app="$(cd "$1" && pwd)"
out="$2"
WIT="${WASMKEEP_WIT:-$root/crates/wasmkeep-wit/wit}"
MOON=${MOON:-moon}
for tool in "$MOON" wit-bindgen wasm-tools; do
  command -v "$tool" >/dev/null || { echo "missing: $tool (see the top of $0)" >&2; exit 1; }
done
[ -f "$WIT/actor.wit" ] || { echo "no actor.wit in $WIT (set WASMKEEP_WIT)" >&2; exit 1; }
gen="$app/gen"
# Ours: the marker below, or the module an earlier build wrote.
if [ -e "$gen" ] && [ ! -f "$gen/.wasmkeep-build" ] \
  && ! grep -qx 'name = "wasmkeep/actor"' "$gen/moon.mod" 2>/dev/null \
  && [ -n "$(ls -A "$gen" 2>/dev/null)" ]; then
  echo "$gen exists and was not written by this script: move it, it would be replaced" >&2
  exit 1
fi
rm -rf "$gen"
mkdir -p "$gen"
touch "$gen/.wasmkeep-build"
wit-bindgen moonbit "$WIT" --world actor --out-dir "$gen" >/dev/null
cp -r "$sdk/wasmkeep" "$gen/wasmkeep"
mkdir -p "$gen/app"
cp "$app"/*.mbt "$app/moon.pkg" "$gen/app/"
world="$gen/gen/world/actor"
handler="$gen/gen/interface/wasi/http/incoming-handler"
cp "$sdk/glue/world.mbt" "$world/wasmkeep_glue.mbt"
cp "$sdk/glue/handler.mbt" "$handler/wasmkeep_glue.mbt"
# The glue's packages also import the SDK and the app (awk and mv, not
# GNU sed's -i).
for pkg in "$world" "$handler"; do
  awk '{ print } $0 == "import {" { print "  \"wasmkeep/actor/wasmkeep\" @wasmkeep,"; print "  \"wasmkeep/actor/app\" @app," }' \
    "$pkg/moon.pkg" > "$pkg/moon.pkg.new"
  mv "$pkg/moon.pkg.new" "$pkg/moon.pkg"
  grep -q '"wasmkeep/actor/app"' "$pkg/moon.pkg" || \
    printf 'import {\n  "wasmkeep/actor/wasmkeep" @wasmkeep,\n  "wasmkeep/actor/app" @app,\n}\n' >> "$pkg/moon.pkg"
done
(cd "$gen" && "$MOON" build --target wasm --release) || {
  echo "moon build failed. If it cannot load the core library: MoonBit's core must be bundled for wasm" >&2
  echo "(moon bundle --target wasm --release in \$MOON_HOME/lib/core; the official installer does it)." >&2
  exit 1
}
core="$gen/_build/wasm/release/build/gen/gen.wasm"
wasm-tools component embed "$WIT" --world actor --encoding utf16 "$core" -o "$gen/embedded.wasm"
wasm-tools component new "$gen/embedded.wasm" -o "$out"
