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
# (MoonBit strings are UTF-16).
#   MoonBit (moon and moonc): https://www.moonbitlang.com/download/
#   cargo install wit-bindgen-cli --version 0.63.0 && cargo install wasm-tools
set -euo pipefail
sdk="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$sdk/../.." && pwd)"
app="$(cd "$1" && pwd)"
out="$2"
WIT="$root/crates/wasmkeep-wit/wit"
MOON=${MOON:-moon}
gen="$app/gen"
rm -rf "$gen"
mkdir -p "$gen"
wit-bindgen moonbit "$WIT" --world actor --out-dir "$gen" >/dev/null 2>&1
cp -r "$sdk/wasmkeep" "$gen/wasmkeep"
mkdir -p "$gen/app"
cp "$app"/*.mbt "$app/moon.pkg" "$gen/app/"
world="$gen/gen/world/actor"
handler="$gen/gen/interface/wasi/http/incoming-handler"
cp "$sdk/glue/world.mbt" "$world/wasmkeep_glue.mbt"
cp "$sdk/glue/handler.mbt" "$handler/wasmkeep_glue.mbt"
# The glue's packages also import the SDK and the app.
for pkg in "$world" "$handler"; do
  sed -i 's|^import {$|import {\n  "wasmkeep/actor/wasmkeep" @wasmkeep,\n  "wasmkeep/actor/app" @app,|' "$pkg/moon.pkg"
  grep -q '"wasmkeep/actor/app"' "$pkg/moon.pkg" || \
    printf 'import {\n  "wasmkeep/actor/wasmkeep" @wasmkeep,\n  "wasmkeep/actor/app" @app,\n}\n' >> "$pkg/moon.pkg"
done
(cd "$gen" && "$MOON" build --target wasm --release)
core="$gen/_build/wasm/release/build/gen/gen.wasm"
wasm-tools component embed "$WIT" --world actor --encoding utf16 "$core" -o "$gen/embedded.wasm"
wasm-tools component new "$gen/embedded.wasm" -o "$out"
