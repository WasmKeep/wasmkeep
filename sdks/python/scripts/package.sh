#!/usr/bin/env bash
# Builds the wheel and checks it the way someone installing it would use it:
#   sdks/python/scripts/package.sh OUT
# OUT is a new or empty directory outside the repository. It receives
# dist/ (the sdist and the wheel), a virtual environment with the wheel
# installed, and counter.wasm: examples/py-counter's app.py built from a
# copy in OUT with the installed package's own command,
# `python -m wasmkeep build`, so the SDK, the WIT and componentize-py are
# the environment's only.
# Needs `build` and `twine` for ${PYTHON:-python3} (pip install build twine)
# and network access: pip fetches the build backend and componentize-py.
set -euo pipefail
[ $# -eq 1 ] || { echo "usage: $0 OUT" >&2; exit 2; }
python=${PYTHON:-python3}
mkdir -p "$1"
out=$(cd "$1" && pwd)
cd "$(dirname "$0")/.."
sdk=$(pwd)
case "$out/" in "$(cd ../.. && pwd)"/*) echo "$out is inside the repository" >&2; exit 2 ;; esac
[ -z "$(ls -A "$out")" ] || { echo "$out is not empty" >&2; exit 2; }

"$python" -m build --outdir "$out/dist" .
"$python" -m twine check --strict "$out"/dist/*

# A pure wheel holding the `wasmkeep` package (modules and WIT) and its
# metadata: no tests, no bytecode, nothing else at the top level.
wheel=$(echo "$out"/dist/wasmkeep-*-py3-none-any.whl)
[ -f "$wheel" ] || { echo "no pure wheel in $out/dist" >&2; exit 1; }
"$python" -I - "$wheel" <<'PY'
import sys, zipfile
names = zipfile.ZipFile(sys.argv[1]).namelist()
bad = [n for n in names if "__pycache__" in n or n.endswith((".pyc", ".pyo"))
       or not (n.startswith("wasmkeep/") or n.split("/")[0].endswith(".dist-info"))]
if bad:
    sys.exit("unexpected files in the wheel: " + ", ".join(bad))
for needed in ("wasmkeep/__init__.py", "wasmkeep/__main__.py", "wasmkeep/wit/actor.wit", "wasmkeep/wit/deps/http.wit"):
    if needed not in names:
        sys.exit("missing from the wheel: " + needed)
for suffix in (".dist-info/licenses/LICENSE", ".dist-info/licenses/NOTICE"):
    if not any(n.endswith(suffix) for n in names):
        sys.exit("missing from the wheel: *" + suffix)
print(f"wheel: {len(names)} files, all under wasmkeep/ or its metadata")
PY

# Install it alone in a fresh environment; componentize-py comes with it as
# the wheel's requirement.
"$python" -m venv "$out/venv"
"$out/venv/bin/python" -m pip install --quiet --disable-pip-version-check "$wheel"
# Where the package landed, without importing it.
pkg=$(cd "$out" && "$out/venv/bin/python" -c 'import importlib.util as u; print(u.find_spec("wasmkeep").submodule_search_locations[0])')
case "$pkg/" in "$out/venv"/*) ;; *) echo "wasmkeep resolved to $pkg, not the environment" >&2; exit 1 ;; esac
diff -r "$sdk/wasmkeep/wit" "$pkg/wit"

mkdir "$out/app"
cp "$sdk/../../examples/py-counter/app.py" "$out/app/"
# The environment is not activated: its python finds its own componentize-py.
(cd "$out/app" && "$out/venv/bin/python" -m wasmkeep build app -o "$out/counter.wasm")
echo "wrote $out/counter.wasm ($(wc -c < "$out/counter.wasm") bytes) from $(basename "$wheel")"
