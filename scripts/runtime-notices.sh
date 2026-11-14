#!/usr/bin/env bash
# Third-party notices for what the counters in other languages embed (the
# release's counter-<lang>.wasm): each language's runtime and libc, with
# its license text, appended to OUT (the release's
# THIRD_PARTY_NOTICES-components.txt, whose crates scripts/notices.sh
# lists). Run after the counters are built, with the tools that built them:
#
#   scripts/runtime-notices.sh OUT
#
# The texts come from the installed tools where they ship one (Zig and the
# libc it bundles, wasi-libc's Debian copyright file, the Go standard
# library and the Go modules go-counter links, componentize-py,
# componentize-qjs) and from deploy/notices/
# where they do not (QuickJS-ng, CPython, TinyGo, the WASI adapter). Those
# files name the tool version they were taken for: a tool at another
# version fails here, so a pin is not moved without its text.
#
# Environment: PYTHON (default python3: the one with componentize-py and
# ziglang), GO (default go: the Go TinyGo builds with), TINYGO (default
# tinygo), WASI_LIBC_COPYRIGHT (default Debian's
# /usr/share/doc/wasi-libc/copyright).
set -euo pipefail
cd "$(dirname "$0")/.."
out=${1:?usage: scripts/runtime-notices.sh OUT}
PYTHON=${PYTHON:-python3}
GO=${GO:-go}
TINYGO=${TINYGO:-tinygo}
wasi_libc=${WASI_LIBC_COPYRIGHT:-/usr/share/doc/wasi-libc/copyright}

fail() { echo "runtime-notices: $*" >&2; exit 1; }
# A vendored text names, in its first line, the version it is for.
vendored() { # file, the version the tool reports
  local file=deploy/notices/$1
  [ -f "$file" ] || fail "$file is missing"
  grep -qF -- "$2" "$file" || fail "$file is not for $2: update it from the source its header names"
  echo "$file"
}
# The version of a Python distribution, and the path of one of its files.
py_version() { "$PYTHON" -I -c 'import importlib.metadata as m, sys; print(m.version(sys.argv[1]))' "$1"; }
py_file() {
  "$PYTHON" -I -c '
import importlib.metadata as m, sys
d = m.distribution(sys.argv[1])
hits = [f for f in (d.files or []) if str(f).replace("\\", "/").endswith(sys.argv[2])]
print(d.locate_file(hits[0]) if hits else "")' "$1" "$2"
}

qjs_dir=$(cd sdks/typescript && node -p 'require("path").dirname(require.resolve("componentize-qjs/package.json"))')
qjs_version=$(node -p 'require(process.argv[1] + "/package.json").version' "$qjs_dir")
cpy_version=$(py_version componentize-py)
cpy_license=$(py_file componentize-py licenses/LICENSE)
zig_version=$(py_version ziglang)
zig_license=$(py_file ziglang licenses/ziglang/LICENSE)
zig_root=$("$PYTHON" -I -c 'import ziglang, os; print(os.path.dirname(ziglang.__file__))')
tinygo_version=$("$TINYGO" version | awk '{print $3}')
goroot=$("$GO" env GOROOT)
adapter=$(sed -n 's/^ADAPTER_VERSION=\([0-9.]*\).*/\1/p' scripts/componentize-core.sh)
go_version=$("$GO" env GOVERSION)
# The one module go-counter links besides the SDK (Apache-2.0 WITH
# LLVM-exception), from the module cache its build filled.
cm_dir=$(cd examples/go-counter && "$GO" list -m -f '{{.Dir}}' go.bytecodealliance.org/cm)
cm_version=$(cd examples/go-counter && "$GO" list -m -f '{{.Version}}' go.bytecodealliance.org/cm)
# Assignments, so that a failed check stops the script (set -e).
quickjs=$(vendored quickjs-ng.txt "componentize-qjs $qjs_version")
cpython=$(vendored cpython.txt "componentize-py $cpy_version")
tinygo=$(vendored tinygo.txt "TinyGo $tinygo_version")
wasi_adapter=$(vendored wasi-adapter.txt "Wasmtime $adapter")

# Section title, then its files.
sections=(
  "TypeScript (counter-typescript.wasm): componentize-qjs $qjs_version, QuickJS-ng"
  "$qjs_dir/LICENSE"
  "$quickjs"
  --
  "Python (counter-python.wasm): componentize-py $cpy_version, CPython"
  "$cpy_license"
  "$cpython"
  --
  "Go (counter-go.wasm): TinyGo $tinygo_version, the Go standard library ($go_version), go.bytecodealliance.org/cm $cm_version"
  "$tinygo"
  "$goroot/LICENSE"
  "$cm_dir/LICENSE"
  --
  "C (counter-c.wasm): wasi-libc, the WASI adapter"
  "$wasi_libc"
  "$wasi_adapter"
  --
  "Zig (counter-zig.wasm): the Zig $zig_version standard library and the wasi-libc it bundles, the WASI adapter"
  "$zig_license"
  "$zig_root/lib/libc/wasi/LICENSE"
  "$zig_root/lib/libc/wasi/LICENSE-MIT"
  "$zig_root/lib/libc/wasi/LICENSE-APACHE"
  "$zig_root/lib/libc/wasi/LICENSE-APACHE-LLVM"
  "$wasi_adapter"
  --
)
title=
files=()
# Written whole, then appended: a missing file leaves OUT as it was.
section=$(mktemp)
trap 'rm -f "$section"' EXIT
{
  echo
  echo '================================================================================'
  echo 'The counters in other languages'
  echo
  echo 'counter-<lang>.wasm, the same counter as counter.wasm in another language,'
  echo 'each contain their language'"'"'s runtime or standard library and a libc. Their'
  echo 'licenses follow, per component. compiler-rt (Apache-2.0 WITH LLVM-exception),'
  echo 'which clang links into the C component, needs no notice in compiled form.'
  echo
  for item in "${sections[@]}"; do
    if [ "$item" = -- ]; then
      echo '================================================================================'
      echo "$title"
      for f in "${files[@]}"; do
        [ -s "$f" ] || fail "$f is missing (for: $title)"
        echo
        echo "--- $(basename "$f")"
        echo
        cat "$f"
      done
      echo
      title=
      files=()
    elif [ -z "$title" ]; then
      title=$item
    else
      files+=("$item")
    fi
  done
} > "$section"
cat "$section" >> "$out"
echo "appended the runtimes' notices to $out"
