#!/usr/bin/env bash
# Packages the C SDK, and the Zig SDK over it, as release archives:
#
#   scripts/package-c-sdk.sh OUT LABEL [REF]
#
# writes OUT/wasmkeep-c-sdk-LABEL.tar.gz and OUT/wasmkeep-zig-sdk-LABEL.tar.gz
# (LABEL: the version, or the release workflow's dev label; REF: the commit
# or tag the READMEs link to, by default HEAD's commit). Each holds the SDK's
# sources, the bindings `wit-bindgen c` generates for the `actor` world (so
# a user needs no wit-bindgen), that WIT, scripts/componentize-core.sh, a
# README (sdks/c/archive, sdks/zig/archive) whose links are absolute,
# LICENSE and the SDK's NOTICE. Needs wit-bindgen 0.63.0.
#
# Then, unless CHECK=0, it checks each archive as a user gets it: extracted
# into an empty directory outside the repository, with only the counter
# example's source beside it, it runs the README's build block as written,
# with a wit-bindgen on PATH that fails, and checks the component against
# the archive's WIT. That needs clang with wasi-libc, Zig 0.17 ($ZIG),
# wasm-tools, curl and tar.
set -euo pipefail
[ $# -ge 2 ] || { echo 'usage: scripts/package-c-sdk.sh OUT LABEL [REF]' >&2; exit 2; }
mkdir -p "$1"
out=$(cd "$1" && pwd)
label=$2
cd "$(dirname "$0")/.."
ref=${3:-$(git rev-parse HEAD)}
wit=crates/wasmkeep-wit/wit
wit_version=$(sed -n 's/^package wasmkeep:actor@\(.*\);$/\1/p' "$wit/actor.wit")
[ -n "$wit_version" ] || { echo "$wit/actor.wit: no package wasmkeep:actor@<version>" >&2; exit 1; }
bindgen=$(wit-bindgen --version 2>/dev/null || true)
case $bindgen in
  "wit-bindgen-cli 0.63.0"*) ;;
  *) echo "needs wit-bindgen-cli 0.63.0 (cargo install wit-bindgen-cli --version 0.63.0); found: ${bindgen:-none}" >&2; exit 1 ;;
esac
epoch=$(git log -1 --format=%ct)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

# README: the template with its placeholders filled; a tarball resolves no
# relative link, and no placeholder stays.
readme() {
  sed -e "s/@VERSION@/$label/g" -e "s/@REF@/$ref/g" -e "s/@WIT_VERSION@/$wit_version/g" "$1" > "$2"
  if grep -nP '@[A-Z_]+@|\]\((?!https://)' "$2"; then
    echo "$1: a placeholder left, or a link that is not absolute" >&2; exit 1
  fi
}
# The C SDK's sources into directory $1, and its bindings into $1/gen.
c_files() {
  mkdir -p "$1/gen"
  install -m 0644 sdks/c/wasmkeep.h sdks/c/wasmkeep.c sdks/c/wasmkeep_kv.h sdks/c/wasmkeep_kv.c \
    sdks/c/wasmkeep_wire.h sdks/c/wasmkeep_wire.c sdks/c/wasmkeep_workflow.h sdks/c/wasmkeep_workflow.c "$1/"
  wit-bindgen c "$wit" --world actor --out-dir "$1/gen" >/dev/null
  chmod 0644 "$1"/gen/*
}
common() {
  cp -r "$wit" "$1/wit"
  mkdir -p "$1/scripts"
  install -m 0755 scripts/componentize-core.sh "$1/scripts/"
  install -m 0644 LICENSE "$1/"
}
pack() {
  tar --sort=name --mtime="@$epoch" --owner=0 --group=0 --numeric-owner --mode=go-w \
    -C "$work" -cf - "$1" | gzip -n -9 > "$out/$1.tar.gz"
  echo "wrote $out/$1.tar.gz"
}

c=wasmkeep-c-sdk-$label
mkdir "$work/$c"
c_files "$work/$c"
common "$work/$c"
install -m 0644 sdks/c/NOTICE "$work/$c/"
readme sdks/c/archive/README.md "$work/$c/README.md"
pack "$c"

zig=wasmkeep-zig-sdk-$label
mkdir -p "$work/$zig/zig"
install -m 0644 sdks/zig/*.zig "$work/$zig/zig/"
c_files "$work/$zig/c"
common "$work/$zig"
install -m 0644 sdks/zig/NOTICE "$work/$zig/"
readme sdks/zig/archive/README.md "$work/$zig/README.md"
pack "$zig"

[ "${CHECK:-1}" = 0 ] && exit 0

# The README's build: its first ```bash block, run as written from the
# directory that holds the extracted archive and the app.
try() { # archive, example source, app file name
  local dir="$work/try-$1" block
  mkdir -p "$dir/no-wit-bindgen"
  printf '#!/bin/sh\necho "wit-bindgen called: the archive should not need it" >&2\nexit 1\n' \
    > "$dir/no-wit-bindgen/wit-bindgen"
  chmod +x "$dir/no-wit-bindgen/wit-bindgen"
  tar -xzf "$out/$1.tar.gz" -C "$dir"
  cp "$2" "$dir/$3"
  block=$(awk '/^```bash$/ { on = 1; next } on && /^```$/ { exit } on' "$dir/$1/README.md")
  [ -n "$block" ] || { echo "$1/README.md: no bash block" >&2; exit 1; }
  (cd "$dir" && PATH="$dir/no-wit-bindgen:$PATH" bash -euo pipefail -c "$block")
  wasm-tools validate "$dir/actor.wasm"
  # The adapter imports WASI interfaces beyond the world (the host links
  # them inert), so check the exports rather than `component targets`.
  wasm-tools component wit "$dir/actor.wasm" > "$dir/actor.wit"
  grep -q 'export on-init: func' "$dir/actor.wit" && grep -q 'export wasi:http/incoming-handler@' "$dir/actor.wit" \
    || { echo "$1: the component does not export the actor world" >&2; exit 1; }
  echo "built $(wc -c < "$dir/actor.wasm") bytes from $1.tar.gz"
}
try "$c" examples/c-counter/counter.c app.c
try "$zig" examples/zig-counter/counter.zig app.zig
