#!/usr/bin/env bash
# Builds counter.wasm with componentize-py (CPython embedded in the component).
#   pip install componentize-py==0.25.1
set -euo pipefail
cd "$(dirname "$0")"
WIT=../../crates/wasmkeep-wit/wit
# The SDK (sdks/python) is plain Python, bundled into the component.
COMPONENTIZE_PY=${COMPONENTIZE_PY:-componentize-py}
$COMPONENTIZE_PY -d "$WIT" -w actor componentize app -p . -p ../../sdks/python -o counter.wasm
echo "wrote $(pwd)/counter.wasm ($(wc -c < counter.wasm) bytes)"
