#!/usr/bin/env bash
# Builds the wasmkeep binary in a container, for Linux distributions other
# than the build machine's:
#
#   scripts/build-portable.sh            glibc 2.28 baseline (AlmaLinux 8):
#                                        runs on any x86_64 or aarch64 Linux
#                                        with glibc 2.28 or newer (RHEL 8,
#                                        Debian 10, Ubuntu 20.04 and later)
#   scripts/build-portable.sh musl       static musl (Alpine): runs anywhere,
#                                        but measured 2.3x the CPU per request
#                                        (docs/benchmarks.md#portable-builds)
#
# The binary is for the build machine's architecture (x86_64 or aarch64):
# the release workflow runs it once on each.
#
# Output: target/portable/<kind>/release/wasmkeep. The source is mounted
# read-only; the toolchain (rust-toolchain.toml's channel) and the cargo
# cache are kept in target/portable/<kind>/cargo. Needs podman or docker.
#
# Base images are pinned by the digest of their multi-architecture index
# (resolved 2026-11-09; its amd64 image is the one pinned on 2026-11-07). To
# refresh, take the tag's index digest, for example with `docker buildx
# imagetools inspect TAG` or `skopeo inspect --raw docker://TAG | sha256sum`.
set -euo pipefail
cd "$(dirname "$0")/.."
kind=${1:-glibc}
engine=$(command -v podman || command -v docker || true)
[ -n "$engine" ] || { echo "needs podman or docker" >&2; exit 1; }
toolchain=$(sed -n 's/^channel = "\(.*\)"/\1/p' rust-toolchain.toml)
case $kind in
  glibc)
    image=docker.io/library/almalinux:8@sha256:2d0f2eb2a83cc09dbc2eb92fade4dbe8f709628149e81aa00aec70630406603b
    deps="dnf -q -y install gcc gcc-c++ make perl curl >/dev/null" ;;
  musl)
    image=docker.io/library/alpine:3.22@sha256:49340b346798875b5592e5d420241f2f9cfba5f47e62ebb1446a1b34734e898f
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
