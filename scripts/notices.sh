#!/usr/bin/env bash
# Third-party notices for the wasmkeep binary: every crate compiled into it
# for the release targets (about.toml), grouped by license, each license's
# full text. A binary distribution must carry this file (docs/legal.md).
#
#   scripts/notices.sh [OUT]      (default: target/notices/THIRD_PARTY_NOTICES.txt)
#   scripts/notices.sh --components OUT DIR...
#
# With --components: one file for the components built for wasm32-wasip2
# from the example directories DIR... (each its own workspace with its own
# lockfile, as the release builds them); a crate in several of them is
# listed once.
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
components=()
if [ "${1:-}" = --components ]; then
  [ $# -ge 3 ] || { echo 'usage: scripts/notices.sh --components OUT DIR...' >&2; exit 2; }
  out=$2
  shift 2
  components=("$@")
else
  out=${1:-target/notices/THIRD_PARTY_NOTICES.txt}
fi
mkdir -p "$(dirname "$out")"
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
if [ ${#components[@]} -eq 0 ]; then
  cargo about generate --locked --fail \
    --manifest-path crates/wasmkeep-cli/Cargo.toml \
    --config about.toml --output-file "$out" about.hbs
  cargo metadata --format-version 1 --locked > "$work/metadata.json"
else
  # cargo-about reads one workspace at a time: its JSON for each component,
  # merged into the same layout as about.hbs gives the binary's file.
  n=0
  for dir in "${components[@]}"; do
    [ -f "$dir/Cargo.toml" ] || { echo "$dir: no Cargo.toml" >&2; exit 1; }
    cargo about generate --locked --fail --format json --target wasm32-wasip2 \
      --manifest-path "$dir/Cargo.toml" --config about.toml --output-file "$work/about-$n.json"
    cargo metadata --format-version 1 --locked --manifest-path "$dir/Cargo.toml" > "$work/metadata-$n.json"
    n=$((n + 1))
  done
  python3 -I -c '
import json, pathlib, sys
work, out, dirs = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3:]
licenses = {}   # (id, text): (name, {(crate, version): repository}), first seen first
for i in range(len(dirs)):
    for lic in json.loads((work / f"about-{i}.json").read_text())["licenses"]:
        _, crates = licenses.setdefault((lic["id"], lic["text"]), (lic["name"], {}))
        for used in lic["used_by"]:
            c = used["crate"]
            crates.setdefault((c["name"], c["version"]), c.get("repository"))
counts = {}
for name, crates in licenses.values():
    counts[name] = counts.get(name, 0) + len(crates)
lines = ["Third-party notices for the example components", "",
         "The example components of a release, built for wasm32-wasip2 from",
         *[f"  {d}" for d in dirs],
         "include the open-source crates listed below. Each license text is",
         "followed by the crates distributed under it, with their versions.",
         "WasmKeep itself is under the Apache License 2.0 (LICENSE, NOTICE).",
         "", "Licenses used:"]
lines += [f"- {name} ({count} crates)" for name, count in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]
lines.append("")
for (lid, text), (name, crates) in licenses.items():
    lines += ["=" * 80, f"{name} ({lid})", "", "Used by:"]
    lines += [f"- {n} {v}" + (f" ({repo})" if repo else "") for (n, v), repo in crates.items()]
    lines += ["", text, ""]
out.write_text("\n".join(lines) + "\n")
packages = {}
for i in range(len(dirs)):
    for pkg in json.loads((work / f"metadata-{i}.json").read_text())["packages"]:
        packages[pkg["id"]] = pkg
(work / "metadata.json").write_text(json.dumps({"packages": list(packages.values())}))
' "$work" "$out" "${components[@]}"
fi
# Two gaps cargo-about leaves, filled from each listed crate's source:
# - a crate whose license file it cannot match (a combined MIT/Apache file,
#   a preamble) gets the SPDX template, "Copyright <year> <owner>"; so a
#   license file whose copyright lines are missing from the output is
#   appended verbatim;
# - Apache-2.0 section 4(d): a dependency's NOTICE file travels with the
#   binary too, and cargo-about does not gather those.
python3 -I -c '
import json, pathlib, re, sys
out = pathlib.Path(sys.argv[1])
text = out.read_text()
entry = re.compile(r"^- (\S+) (\d+\.\d+\.\d+\S*)(?: \(\S+\))?$", re.M)
listed = set(entry.findall(text))
squash = lambda s: " ".join(s.split())
present = squash(text)
licenses, notices = [], []
for pkg in json.loads(pathlib.Path(sys.argv[2]).read_text())["packages"]:
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
' "$out" "$work/metadata.json"
echo "wrote $out"
