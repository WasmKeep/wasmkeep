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
#
# The toolchain is installed by a pinned rustup-init, checked by SHA-256 before
# it runs (no installer script is piped into a shell). To refresh, take the
# version's checksums from
# https://static.rust-lang.org/rustup/archive/<version>/<target>/rustup-init.sha256
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
# rustup-init for the container's architecture (the build machine's), the gnu
# build for glibc and the static musl build for Alpine.
RUSTUP_VERSION=1.29.3
case $(uname -m) in
  x86_64 | amd64) arch=x86_64 ;;
  aarch64 | arm64) arch=aarch64 ;;
  *) echo "unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac
case $arch-$kind in
  x86_64-glibc)  triple=x86_64-unknown-linux-gnu;    RUSTUP_SHA256=b1c8cb14e30889e7e2999e6b69e62d9e9b9ffc0f4df8180564d306d9680e6c55 ;;
  aarch64-glibc) triple=aarch64-unknown-linux-gnu;   RUSTUP_SHA256=9f608c142e0e38a78957d0661ba1dfaf0b6fb37184cfaa4b9a0aa57af6184282 ;;
  x86_64-musl)   triple=x86_64-unknown-linux-musl;   RUSTUP_SHA256=1552169aba2836777136ccff7a381e95c17233b5d8250cd5e10ae3cb971bfcfc ;;
  aarch64-musl)  triple=aarch64-unknown-linux-musl;  RUSTUP_SHA256=956deacdc27ab82ae7ac821d5d3e9e95e4795d476d17efe99743bb8531ca1031 ;;
esac
RUSTUP_URL=https://static.rust-lang.org/rustup/archive/$RUSTUP_VERSION/$triple/rustup-init
out=$PWD/target/portable/$kind
mkdir -p "$out/target" "$out/cargo"
"$engine" run --rm \
  -v "$PWD:/src:ro,Z" -v "$out/target:/target:Z" -v "$out/cargo:/cargo:Z" \
  -e CARGO_TARGET_DIR=/target -e CARGO_HOME=/cargo -e RUSTUP_HOME=/cargo/rustup \
  -e RUSTUP_URL="$RUSTUP_URL" -e RUSTUP_SHA256="$RUSTUP_SHA256" \
  "$image" sh -euc "
    $deps
    if [ ! -x /cargo/bin/cargo ]; then
      curl -sSfL -o /tmp/rustup-init \"\$RUSTUP_URL\"
      echo \"\$RUSTUP_SHA256  /tmp/rustup-init\" | sha256sum -c >/dev/null
      chmod +x /tmp/rustup-init
      /tmp/rustup-init -y -q --profile minimal --default-toolchain $toolchain --no-modify-path
    fi
    cd /src && /cargo/bin/cargo build --release --locked -p wasmkeep-cli"
mkdir -p "$out/release"
cp "$out/target/release/wasmkeep" "$out/release/wasmkeep"
echo "built $out/release/wasmkeep"
