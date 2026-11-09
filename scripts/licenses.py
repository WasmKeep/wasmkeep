"""Lists Rust dependencies with their licenses as Markdown (stdin: `cargo
metadata` JSON); exits 1 if one is outside the allowed set."""
import json
import re
import sys

ALLOWED = {
    "MIT", "MIT-0", "Apache-2.0", "Apache-2.0 WITH LLVM-exception",
    "BSD-2-Clause", "BSD-3-Clause", "ISC", "Zlib", "Unicode-3.0",
    "Unlicense", "BSL-1.0", "CC0-1.0", "0BSD", "CDLA-Permissive-2.0",
}


def allowed(expr: str) -> bool:
    """An SPDX expression is fine if some choice it offers is all allowed."""
    expr = expr.replace("/", " OR ").strip()
    if not expr:
        return False
    # Normalize "A WITH B" into one token, then evaluate OR/AND with ().
    tokens = re.findall(r"\(|\)|[A-Za-z0-9.\-]+(?:\s+WITH\s+[A-Za-z0-9.\-]+)?", expr)
    pos = 0

    def parse_or():
        nonlocal pos
        value = parse_and()
        while pos < len(tokens) and tokens[pos] == "OR":
            pos += 1
            value = parse_and() or value
        return value

    def parse_and():
        nonlocal pos
        value = parse_atom()
        while pos < len(tokens) and tokens[pos] == "AND":
            pos += 1
            value = parse_atom() and value
        return value

    def parse_atom():
        nonlocal pos
        tok = tokens[pos]
        pos += 1
        if tok == "(":
            value = parse_or()
            pos += 1  # ")"
            return value
        return re.sub(r"\s+", " ", tok) in ALLOWED

    return parse_or()


meta = json.load(sys.stdin)
ours = set(meta["workspace_members"])
rows, bad = [], []
for p in sorted(meta["packages"], key=lambda p: (p["name"], p["version"])):
    if p["id"] in ours:
        continue
    lic = p.get("license") or ""
    if not allowed(lic):
        bad.append(f"{p['name']} {p['version']}: {lic or 'no license field'}")
    rows.append(f"| {p['name']} | {p['version']} | {lic} | {p.get('repository') or ''} |")
print("# Third-party licenses\n")
print("Rust crates WasmKeep is built from, with their licenses (from Cargo.lock).\n")
print("| Crate | Version | License | Source |\n| --- | --- | --- | --- |")
print("\n".join(rows))
if bad:
    print("licenses outside the allowed set:\n  " + "\n  ".join(bad), file=sys.stderr)
    sys.exit(1)
