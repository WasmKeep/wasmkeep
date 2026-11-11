#!/usr/bin/env bash
# Copies the WIT world from crates/wasmkeep-wit into the package, so an
# installed wheel carries the world its modules are written against.
# scripts/check.sh and CI fail when the copy differs.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf wasmkeep/wit
cp -R ../../crates/wasmkeep-wit/wit wasmkeep/wit
