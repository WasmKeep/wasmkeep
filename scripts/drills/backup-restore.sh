#!/usr/bin/env bash
# A bounded fixture drill, or a read-only backup of the soak's deployed fixture.
set -euo pipefail
cd "$(dirname "$0")/../.."
export CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-4}
export NEXTEST_TEST_THREADS=${NEXTEST_TEST_THREADS:-4}
export TOKIO_WORKER_THREADS=2
if [ "${DRILL_SKIP_BUILD:-0}" != 1 ]; then
  cargo build -p wasmkeep-cli --locked -j 4
  cargo build --manifest-path crates/wasmkeep-server/tests/guests/test-actor/Cargo.toml \
    --release --locked --target wasm32-wasip2 -j 4
  cargo build --manifest-path crates/wasmkeep-server/tests/guests/blob-actor/Cargo.toml \
    --release --locked --target wasm32-wasip2 -j 4
fi
export DRILL_BLOB_WASM=${DRILL_BLOB_WASM:-crates/wasmkeep-server/tests/guests/blob-actor/target/wasm32-wasip2/release/blob_actor.wasm}
export DRILL_BIN=${DRILL_BIN:-${CARGO_TARGET_DIR:-target}/debug/wasmkeep}
export DRILL_WASM=${DRILL_WASM:-crates/wasmkeep-server/tests/guests/test-actor/target/wasm32-wasip2/release/test_actor.wasm}
exec python3 -I scripts/drills/backup_restore.py "$@"
