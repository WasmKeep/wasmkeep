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
# Two gaps cargo-about leaves, filled from each listed crate's source:
# - a crate whose license file it cannot match (a combined MIT/Apache file,
#   a preamble) gets the SPDX template, "Copyright <year> <owner>"; so a
#   license file whose copyright lines are missing from the output is
#   appended verbatim;
# - Apache-2.0 section 4(d): a dependency's NOTICE file travels with the
#   binary too, and cargo-about does not gather those.
cargo metadata --format-version 1 --locked | python3 -I -c '
import json, pathlib, re, sys
out = pathlib.Path(sys.argv[1])
text = out.read_text()
entry = re.compile(r"^- (\S+) (\d+\.\d+\.\d+\S*)(?: \(\S+\))?$", re.M)
listed = set(entry.findall(text))
squash = lambda s: " ".join(s.split())
present = squash(text)
licenses, notices = [], []
for pkg in json.load(sys.stdin)["packages"]:
    if (pkg["name"], pkg["version"]) not in listed:
        continue
    root = pathlib.Path(pkg["manifest_path"]).parent
    for f in sorted(root.iterdir()):
        if not f.is_file():
            continue
        upper = f.name.upper()
        body = f.read_text(errors="replace").strip()
        if upper.startswith("NOTICE"):
            notices.append((pkg["name"], pkg["version"], f.name, body))
        elif upper.startswith(("LICENSE", "LICENCE", "COPYING")):
            lines = [squash(l) for l in body.splitlines()
                     if re.search(r"copyright", l, re.I) and re.search(r"\d{4}", l)
                     and not re.search(r"\[yyyy\]|<year>", l)]
            if any(l not in present for l in lines):
                licenses.append((pkg["name"], pkg["version"], f.name, body))
with out.open("a") as w:
    for title, found in (("License files of crates whose copyright lines are not above", licenses),
                         ("NOTICE files of the crates above", notices)):
        if found:
            w.write("=" * 80 + f"\n{title}\n")
            for name, version, fname, body in found:
                w.write(f"\n{name} {version}, {fname}:\n\n{body}\n")
print(f"{len(listed)} crates; {len(licenses)} license file(s) and {len(notices)} NOTICE file(s) appended", file=sys.stderr)
' "$out"
echo "wrote $out"
