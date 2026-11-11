#!/usr/bin/env bash
# Packages the Rust SDK as a registry would receive it, then builds
# examples/counter for wasm32-wasip2 against the unpacked package, from a
# directory outside the repository: what someone who never cloned it gets.
# Publishes nothing. Arguments are passed to `cargo package`
# (--allow-dirty while trying an uncommitted change).
set -euo pipefail
cd "$(dirname "$0")/.."
# The crate carries its own copy of the WIT: it must be the host's.
diff -r crates/wasmkeep-wit/wit crates/wasmkeep-sdk/wit || {
  echo 'crates/wasmkeep-sdk/wit differs from crates/wasmkeep-wit/wit' >&2; exit 1;
}
cargo package -p wasmkeep-sdk --locked "$@"
version=$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)
# Wherever cargo keeps its target directory on this machine.
target=$(cargo metadata --no-deps --format-version 1 \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')
package="$target/package/wasmkeep-sdk-$version"
[ -f "$package/wit/actor.wit" ] || { echo "$package has no wit/actor.wit" >&2; exit 1; }

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir "$work/counter"
cp -r examples/counter/Cargo.toml examples/counter/Cargo.lock examples/counter/src "$work/counter/"
# The same toolchain and target as in the repository.
cp rust-toolchain.toml "$work/counter/"
sed -i.orig "s|path = \"../../crates/wasmkeep-sdk\"|path = \"$package\"|" "$work/counter/Cargo.toml"
grep -qF "path = \"$package\"" "$work/counter/Cargo.toml" || {
  echo 'examples/counter/Cargo.toml no longer names the SDK by the expected path' >&2; exit 1;
}
# --locked: the package's own requirements accept the versions the example
# is locked to.
(cd "$work/counter" && cargo build --release --target wasm32-wasip2 --locked)
wasm="$work/counter/target/wasm32-wasip2/release/counter.wasm"
[ -s "$wasm" ] || { echo 'the consumer build produced no component' >&2; exit 1; }
echo "built $(wc -c < "$wasm") bytes against $package"
