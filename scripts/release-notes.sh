#!/bin/sh
# Prints the text of one version's section of CHANGELOG.md: the lines
# between its `## <version>` heading and the next `## ` heading. Prints
# nothing when there is no such section. Links relative to the repository
# are made absolute, at the version's tag, since a GitHub release text
# resolves nothing relative.
# Usage: scripts/release-notes.sh 0.1.0
set -eu
version=${1:?usage: release-notes.sh <version>}
cd "$(dirname "$0")/.."
base=https://github.com/WasmKeep/wasmkeep/blob/v$version
awk -v v="$version" '
  /^## / { if (in_section) exit; in_section = ($2 == v); next }
  in_section
' CHANGELOG.md \
  | sed -E "s|\]\(([^):/#][^):]*)\)|](${base}/\1)|g" \
  | sed -e '/./,$!d' | sed -e ':a' -e '/^\n*$/{$d;N;ba' -e '}'
