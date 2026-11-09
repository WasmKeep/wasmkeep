#!/usr/bin/env bash
# Builds chat_room.wasm with componentize-py (CPython embedded in the component).
#   pip install componentize-py==0.25.1
set -euo pipefail
cd "$(dirname "$0")"
WIT=../../crates/wasmkeep-wit/wit
# The SDK (sdks/python) is plain Python, bundled into the component.
COMPONENTIZE_PY=${COMPONENTIZE_PY:-componentize-py}
$COMPONENTIZE_PY -d "$WIT" -w actor componentize app -p . -p ../../sdks/python -o chat_room.wasm
echo "wrote $(pwd)/chat_room.wasm ($(wc -c < chat_room.wasm) bytes)"
