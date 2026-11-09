#!/usr/bin/env bash
# CycloneDX SBOM of the wasmkeep binary: every crate compiled into it (build
# dependencies included, dev-dependencies and other workspace members not),
# from Cargo.lock for one target, with versions, licenses and package URLs.
#
#   scripts/sbom.sh [TARGET]      (default: the host target)
#
# Writes target/sbom/wasmkeep-<TARGET>.cdx.json. Uses cargo-cyclonedx, an
# external cargo subcommand (nothing added to the crates' manifests):
#   cargo install cargo-cyclonedx --version 0.5.9 --locked
# The timestamp is the last commit's, so the same commit gives the same file.
set -euo pipefail
cd "$(dirname "$0")/.."
if ! cargo cyclonedx --version >/dev/null 2>&1; then
  echo 'missing: cargo-cyclonedx; run cargo install cargo-cyclonedx --version 0.5.9 --locked' >&2
  exit 1
fi
target=${1:-$(rustc -vV | sed -n 's/^host: //p')}
out=target/sbom/wasmkeep-$target.cdx.json
# cargo-cyclonedx writes one <bin>_bin.cdx.json next to every binary crate's
# manifest in the workspace; keep the wasmkeep one, remove the others.
cleanup() { rm -f crates/*/*_bin.cdx.json sim/*_bin.cdx.json bench/compare/*_bin.cdx.json; }
trap cleanup EXIT
cleanup
SOURCE_DATE_EPOCH=$(git log -1 --format=%ct) cargo cyclonedx --quiet \
  --manifest-path crates/wasmkeep-cli/Cargo.toml --describe binaries \
  --format json --spec-version 1.5 --target "$target"
mkdir -p target/sbom
mv crates/wasmkeep-cli/wasmkeep_bin.cdx.json "$out"
count=$(python3 -I -c 'import json, sys; print(len(json.load(open(sys.argv[1]))["components"]))' "$out")
echo "$out: $count components"
