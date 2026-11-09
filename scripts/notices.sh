#!/usr/bin/env bash
# Third-party notices for the wasmkeep binary: every crate compiled into it
# for the release targets (about.toml), grouped by license, each license's
# full text. A binary distribution must carry this file (docs/legal.md).
#
#   scripts/notices.sh [OUT]      (default: target/notices/THIRD_PARTY_NOTICES.txt)
#
# Uses cargo-about, an external cargo subcommand (nothing added to the
# crates' manifests):
#   cargo install cargo-about --version 0.9.3 --locked
# Fails on a license outside the accepted set, or a crate whose license
# cargo-about cannot establish.
set -euo pipefail
cd "$(dirname "$0")/.."
if ! cargo about --version >/dev/null 2>&1; then
  echo 'missing: cargo-about; run cargo install cargo-about --version 0.9.3 --locked' >&2
  exit 1
fi
out=${1:-target/notices/THIRD_PARTY_NOTICES.txt}
mkdir -p "$(dirname "$out")"
cargo about generate --locked --fail \
  --manifest-path crates/wasmkeep-cli/Cargo.toml \
  --config about.toml --output-file "$out" about.hbs
# Apache-2.0 section 4(d): a dependency's NOTICE file travels with the
# binary too. cargo-about gathers licenses, not these, so append each listed
# crate's NOTICE* file from its source.
cargo metadata --format-version 1 --locked | python3 -I -c '
import json, pathlib, re, sys
out = pathlib.Path(sys.argv[1])
listed = set(re.findall(r"^- (\S+) (\d\S*)", out.read_text(), re.M))
found = []
for pkg in json.load(sys.stdin)["packages"]:
    if (pkg["name"], pkg["version"]) not in listed:
        continue
    root = pathlib.Path(pkg["manifest_path"]).parent
    for f in sorted(root.iterdir()):
        if f.is_file() and f.name.upper().startswith("NOTICE"):
            found.append((pkg["name"], pkg["version"], f.read_text().strip()))
if found:
    with out.open("a") as w:
        w.write("=" * 80 + "\nNOTICE files of the crates above\n")
        for name, version, text in found:
            w.write(f"\n{name} {version}:\n\n{text}\n")
print(f"{len(found)} NOTICE file(s) appended", file=sys.stderr)
' "$out"
echo "$out: $(grep -c '^- .* [0-9][^ ]*\( (.*)\)\?$' "$out") crate entries"
