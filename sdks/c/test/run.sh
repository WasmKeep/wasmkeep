#!/usr/bin/env bash
# Native unit tests of the C SDK's kv, queue and workflow clients (sdks/c/test/wire_test.c):
# the pure parts (framing, codecs, shard hash, read cache, workflow status) and the clients over a
# fake actors.call, compiled with the host's C compiler. No WASI toolchain needed.
#   CC=gcc ./run.sh to use another compiler.
set -euo pipefail
cd "$(dirname "$0")"
CC=${CC:-cc}
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
# Sanitizers when the compiler has them; plain -O1 otherwise.
san=(-fsanitize=address,undefined -fno-omit-frame-pointer)
if ! echo 'int main(void){return 0;}' | "$CC" -x c - "${san[@]}" -o "$out/probe" 2>/dev/null; then san=(); fi
"$CC" -std=c11 -O1 -g -Wall -Wextra -Werror -I.. "${san[@]}" \
  wire_test.c ../wasmkeep_wire.c ../wasmkeep_kv.c ../wasmkeep_workflow.c -o "$out/wire_test"
"$out/wire_test"
