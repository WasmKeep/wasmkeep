#!/usr/bin/env bash
# Builds counter.wasm with the MoonBit SDK's build script (see
# sdks/moonbit/build.sh for the tools).
set -euo pipefail
cd "$(dirname "$0")"
../../sdks/moonbit/build.sh . counter.wasm
echo "wrote $(pwd)/counter.wasm ($(wc -c < counter.wasm) bytes)"
