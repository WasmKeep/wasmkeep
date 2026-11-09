#!/usr/bin/env bash
# Builds the wasmkeep binary in a container, for Linux distributions other
# than the build machine's:
#
#   scripts/build-portable.sh            glibc 2.28 baseline (AlmaLinux 8):
#                                        runs on any x86_64 Linux with glibc
#                                        2.28 or newer (RHEL 8, Debian 10,
#                                        Ubuntu 20.04 and later)
#   scripts/build-portable.sh musl       static musl (Alpine): runs anywhere,
#                                        but measured 2.3x the CPU per request
#                                        (docs/benchmarks.md#portable-builds)
#
# Output: target/portable/<kind>/release/wasmkeep. The source is mounted
# read-only; the toolchain (rust-toolchain.toml's channel) and the cargo
# cache are kept in target/portable/<kind>/cargo. Needs podman or docker.
#
# Base images are pinned by the digest pulled on 2026-11-07 (amd64). To
# refresh, pull the tag and take `podman image inspect --format
# '{{.Digest}}'`.
set -euo pipefail
cd "$(dirname "$0")/.."
kind=${1:-glibc}
engine=$(command -v podman || command -v docker || true)
[ -n "$engine" ] || { echo "needs podman or docker" >&2; exit 1; }
toolchain=$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)
case $kind in
  glibc)
    image=docker.io/library/almalinux:8@sha256:1965cb10f9c392766b2d544d3c1b997eb008d0e413c808e6a20086e87c802a80
    deps="dnf -q -y install gcc gcc-c++ make perl curl >/dev/null" ;;
  musl)
    image=docker.io/library/alpine:3.22@sha256:9db2934886712d877c932e07249c75a59e6ca27adfbccc05261987a17bef2d63
    deps="apk add -q bash gcc g++ musl-dev make perl curl >/dev/null" ;;
  *) echo "usage: $0 [glibc|musl]" >&2; exit 2 ;;
esac
out=$PWD/target/portable/$kind
mkdir -p "$out/target" "$out/cargo"
"$engine" run --rm \
  -v "$PWD:/src:ro,Z" -v "$out/target:/target:Z" -v "$out/cargo:/cargo:Z" \
  -e CARGO_TARGET_DIR=/target -e CARGO_HOME=/cargo -e RUSTUP_HOME=/cargo/rustup \
  "$image" sh -euc "
    $deps
    [ -x /cargo/bin/cargo ] || curl -sSf https://sh.rustup.rs \
      | sh -s -- -y -q --profile minimal --default-toolchain $toolchain --no-modify-path
    cd /src && /cargo/bin/cargo build --release --locked -p wasmkeep-cli"
mkdir -p "$out/release"
cp "$out/target/release/wasmkeep" "$out/release/wasmkeep"
echo "built $out/release/wasmkeep"
