#!/usr/bin/env bash
# Builds chat_room.wasm with the MoonBit SDK's build script (see
# sdks/moonbit/build.sh for the tools).
set -euo pipefail
cd "$(dirname "$0")"
../../sdks/moonbit/build.sh . chat_room.wasm
echo "wrote $(pwd)/chat_room.wasm ($(wc -c < chat_room.wasm) bytes)"
