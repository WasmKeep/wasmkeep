#!/usr/bin/env bash
# Builds counter.wasm with componentize-py (CPython embedded in the component).
#   pip install componentize-py==0.25.1
set -euo pipefail
cd "$(dirname "$0")"
# The SDK (sdks/python) is plain Python, bundled into the component; its
# build command runs componentize-py on the WIT world the SDK carries. From
# an installed package, the same command needs no PYTHONPATH.
PYTHONPATH=../../sdks/python "${PYTHON:-python3}" -m wasmkeep build app -o counter.wasm
