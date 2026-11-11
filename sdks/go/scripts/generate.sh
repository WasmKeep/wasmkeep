#!/usr/bin/env bash
# Regenerates what the module ships from crates/wasmkeep-wit/wit:
#   internal/gen  the wit-bindgen-go bindings the wasmkeep package wraps
#   wit           the WIT TinyGo encodes the component against: the actor
#                 world plus the wasi:filesystem imports TinyGo's runtime
#                 makes whether or not a program uses them (the host
#                 provides them inert: no directories)
# Both are committed, so an app needs only TinyGo to build; CI runs this
# and fails on a difference.
#   go install go.bytecodealliance.org/cmd/wit-bindgen-go@v0.7.0
set -euo pipefail
cd "$(dirname "$0")/.."
WIT=../../crates/wasmkeep-wit/wit
WIT_BINDGEN_GO=${WIT_BINDGEN_GO:-wit-bindgen-go}
rm -rf internal/gen wit
mkdir -p internal/gen
cp -r "$WIT" wit
cat > wit/tinygo.wit <<'WIT'
package wasmkeep:actor@0.2.0;

/// The actor world plus what TinyGo's runtime imports.
world actor-tinygo {
    include actor;
    import wasi:filesystem/types@0.2.12;
    import wasi:filesystem/preopens@0.2.12;
}
WIT
"$WIT_BINDGEN_GO" generate --world actor --out internal/gen "$WIT" >/dev/null
