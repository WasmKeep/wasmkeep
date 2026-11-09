#!/usr/bin/env bash
# Builds jobs.wasm with componentize-py (CPython embedded in the component).
#   pip install componentize-py==0.25.1
set -euo pipefail
cd "$(dirname "$0")"
WIT=../../crates/wasmkeep-wit/wit
# The SDK (sdks/python) is plain Python, bundled into the component.
COMPONENTIZE_PY=${COMPONENTIZE_PY:-componentize-py}
$COMPONENTIZE_PY -d "$WIT" -w actor componentize app -p . -p ../../sdks/python -o jobs.wasm
echo "wrote $(pwd)/jobs.wasm ($(wc -c < jobs.wasm) bytes)"
