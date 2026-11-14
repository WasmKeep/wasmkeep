#!/usr/bin/env bash
# The package as a project outside this repository gets it: packs the SDK
# with `npm pack`, installs the tarball into an empty directory with a copy
# of examples/ts-counter (its sources and tsconfig.json), builds the actor
# with `npx wasmkeep-ts build` on both engines and type-checks it with
# `tsc --noEmit`, then builds examples/ts-todo-front's front
# (`@wasmkeep/sdk/front`, `--world front`). Nothing is published.
#
#   sdks/typescript/scripts/pack-test.sh [directory]
#
# The directory must be empty or absent, and outside the repository, so that
# nothing resolves to the checkout; without one, a temporary directory is
# used and removed. Needs node, npm and network access to the npm registry.
set -euo pipefail

sdk=$(cd "$(dirname "$0")/.." && pwd -P)
repo=$(cd "$sdk/../.." && pwd -P)
example="$repo/examples/ts-counter"

if [ $# -gt 0 ]; then
  mkdir -p "$1"
  work=$(cd "$1" && pwd -P)
  [ -z "$(ls -A "$work")" ] || { echo "pack-test: $work is not empty" >&2; exit 1; }
else
  work=$(cd "$(mktemp -d)" && pwd -P)
  trap 'rm -rf "$work"' EXIT
fi
case "$work/" in
  "$repo"/*) echo "pack-test: $work is inside the repository" >&2; exit 1 ;;
esac

mkdir "$work/pack" "$work/app"
(cd "$sdk" && npm pack --loglevel=error --pack-destination "$work/pack" >/dev/null)
tarball=$(echo "$work"/pack/*.tgz)
[ -f "$tarball" ] || { echo 'pack-test: npm pack wrote no tarball' >&2; exit 1; }
echo "packed $(basename "$tarball")"

# The compiler the SDK itself is checked with: one pin, in its package.json.
typescript=$(node -p 'require(process.argv[1]).devDependencies.typescript' "$sdk/package.json")

cd "$work/app"
cp -R "$example/src" "$example/tsconfig.json" .
# A package.json of its own, so that npm installs here and not in a parent.
printf '{ "name": "pack-test", "private": true, "type": "module" }\n' > package.json
npm install --loglevel=error --no-audit --no-fund "$tarball" "typescript@$typescript"
# What the NOTICE names, and what a consumer imports, is in the package.
for f in LICENSE NOTICE README.md wit/deps types/actor.d.ts src/front.ts; do
  [ -e "node_modules/@wasmkeep/sdk/$f" ] || { echo "pack-test: $f is not in the package" >&2; exit 1; }
done

# Nothing may come from the checkout: a link would hide a file the tarball lacks.
installed=$(node -p 'require("node:fs").realpathSync("node_modules/@wasmkeep/sdk")')
case "$installed/" in
  "$work"/*) ;;
  *) echo "pack-test: the SDK resolved to $installed, outside $work" >&2; exit 1 ;;
esac

version=$(node -p 'require(process.argv[1]).version' "$sdk/package.json")
[ "$(npx --no -- wasmkeep-ts --version)" = "$version" ] || { echo 'pack-test: --version is not the package version' >&2; exit 1; }
npx --no -- wasmkeep-ts --help >/dev/null

npx --no -- wasmkeep-ts build src/index.ts -o counter.wasm
npx --no -- wasmkeep-ts build src/index.ts -o counter-starlingmonkey.wasm --engine starlingmonkey
[ -s counter.wasm ]
[ -s counter-starlingmonkey.wasm ]
npx --no -- tsc --noEmit
mkdir front
cp -R "$repo/examples/ts-todo-front/src" front/
npx --no -- wasmkeep-ts build front/src/index.ts --world front -o front.wasm
[ -s front.wasm ]
echo 'pack-test: the packed SDK installs, builds on both engines, type-checks and builds a front'
