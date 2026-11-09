#!/usr/bin/env bash
# Lists every Rust dependency with its license (Markdown on stdout) and fails
# if one is outside the allowed set. Run before a release; ship the output
# (THIRD_PARTY_LICENSES.md) with binaries. See docs/legal.md.
set -euo pipefail
cd "$(dirname "$0")/.."
cargo metadata --format-version 1 --locked | python3 scripts/licenses.py
