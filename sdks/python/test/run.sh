#!/usr/bin/env bash
# The Python SDK's unit tests, under plain CPython: no componentize-py and no
# host (test/host_stub.py stands in for the generated bindings).
set -euo pipefail
cd "$(dirname "$0")/.."
exec "${PYTHON:-python3}" -I -B -m unittest discover -s test "$@"
