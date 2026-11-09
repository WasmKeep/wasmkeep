#!/usr/bin/env bash
# Diagnose prerequisites without installing tools or changing test runners.
set -euo pipefail
cd "$(dirname "$0")/.."
mode=${1:---full}
case "$mode" in --full|--rust) ;; *) echo 'usage: scripts/doctor.sh [--rust|--full]' >&2; exit 2 ;; esac
missing=0
need() { echo "missing: $*" >&2; missing=1; }
if ! cargo nextest --version >/dev/null 2>&1; then
  need 'cargo-nextest; install cargo-nextest (tests require process isolation).'
fi
# Query the active compiler: Rust may be supplied by the OS, without rustup.
target_libdir=$(rustc --print target-libdir --target wasm32-wasip2 2>/dev/null || true)
if [ -z "$target_libdir" ] || ! compgen -G "$target_libdir/libcore-*.rlib" >/dev/null; then
  need 'wasm32-wasip2 standard library for the active compiler; install that target (rustup target add wasm32-wasip2 with rustup).'
fi
if [ "$mode" = --full ]; then
  if ! command -v mdbook >/dev/null 2>&1; then
    need 'mdbook; run cargo install mdbook --version 0.5.4 --locked.'
  fi
  msrv=$(sed -n 's/^rust-version = "\([^"]*\)"/\1/p' Cargo.toml)
  if [ -z "$msrv" ]; then
    need 'workspace rust-version in Cargo.toml.'
  elif ! rustup toolchain list 2>/dev/null | grep -qE "^${msrv//./\\.}([.-]|[[:space:]])"; then
    need "Rust $msrv; run rustup toolchain install $msrv --profile minimal."
  fi
  if ! cargo deny --version >/dev/null 2>&1; then need 'cargo-deny; install cargo-deny.'; fi
  # The examples and languages step (scripts/languages.sh); skip it with
  # CHECK_SKIP_LANGS=1 on a machine without them.
  if [ "${CHECK_SKIP_LANGS:-}" != 1 ]; then
    for tool in wasm-tools wit-bindgen clang node npm go tinygo wit-bindgen-go componentize-py; do
      if ! command -v "$tool" >/dev/null 2>&1; then
        need "$tool for the examples in every language (see .github/workflows/ci.yml, job languages, for versions), or set CHECK_SKIP_LANGS=1."
      fi
    done
    # Zig as the examples' build.sh run it: $ZIG (default zig), or the
    # ziglang Python package (ZIG="python3 -m ziglang").
    # shellcheck disable=SC2086 # ZIG may hold a command and its arguments
    if ! ${ZIG:-zig} version >/dev/null 2>&1 && ! python3 -m ziglang version >/dev/null 2>&1; then
      need 'zig for the examples in every language (zig on PATH, or pip install ziglang and ZIG="python3 -m ziglang"), or set CHECK_SKIP_LANGS=1.'
    fi
  fi
fi
exit "$missing"
