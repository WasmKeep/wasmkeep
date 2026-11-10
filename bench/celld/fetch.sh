#!/usr/bin/env bash
# Downloads the celld release bench/compare runs (target/celld/<version>/celld)
# for this machine, and checks its build provenance with
# `gh attestation verify` before unpacking it: the binary must have been
# built by denoland/celld's own release workflow. Needs gh, signed in.
# Usage: bench/celld/fetch.sh [version]   (default: the pinned version)
set -euo pipefail
cd "$(git -C "$(dirname "$0")" rev-parse --show-toplevel)"
version=${1:-v0.6.3}
case "$(uname -sm)" in
  "Linux x86_64") asset=celld-x86_64-unknown-linux-gnu.gz ;;
  "Linux aarch64") asset=celld-aarch64-unknown-linux-gnu.gz ;;
  "Darwin arm64") asset=celld-aarch64-apple-darwin.gz ;;
  *) echo "no celld release for $(uname -sm)" >&2; exit 1 ;;
esac
dir=target/celld/$version
mkdir -p "$dir"
gh release download "$version" --repo denoland/celld --pattern "$asset" \
  --pattern LICENSE --pattern NOTICE --dir "$dir" --clobber
gh attestation verify "$dir/$asset" --repo denoland/celld \
  --signer-workflow denoland/celld/.github/workflows/release.yml
gzip -dc "$dir/$asset" > "$dir/celld.tmp"
chmod +x "$dir/celld.tmp" && mv "$dir/celld.tmp" "$dir/celld"
"$dir/celld" --version
