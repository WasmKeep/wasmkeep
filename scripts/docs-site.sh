#!/usr/bin/env bash
# Builds the docs site (docs/ through mdBook) into target/book.
#
# Pages are the files listed in docs/SUMMARY.md. Every relative link in them
# is checked: a link to a missing file fails the build. Links to files that
# are not pages (code, examples, scripts) are rewritten
# to the repository on GitHub, so they work on the site too.
#
#   cargo install mdbook --version 0.5.4 --locked
set -euo pipefail
cd "$(dirname "$0")/.."
REPO_URL=${REPO_URL:-https://github.com/wasmkeep/wasmkeep}
STAGE=target/book-src
rm -rf "$STAGE" target/book
mkdir -p "$STAGE/docs"
cp -r docs/. "$STAGE/docs/"
mv "$STAGE/docs/book.toml" "$STAGE/book.toml"
python3 - "$STAGE/docs" "$REPO_URL" <<'PY'
import os, re, sys
stage, repo = sys.argv[1], sys.argv[2]
docs = os.path.abspath("docs")
root = os.path.abspath(".")
summary = open(os.path.join(docs, "SUMMARY.md")).read()
pages = {os.path.normpath(p) for p in re.findall(r"\]\(([^)#]+\.md)\)", summary)}
link = re.compile(r"(?<!!)\[([^\]]*)\]\(([^)\s]+)\)")
errors = []
for page in sorted(pages | {"SUMMARY.md"}):
    src = os.path.join(docs, page)
    if not os.path.exists(src):
        errors.append(f"SUMMARY.md lists a missing page: {page}")
        continue
    text = open(src).read()
    in_code = False
    out = []
    for line in text.split("\n"):
        if line.lstrip().startswith("```"):
            in_code = not in_code
        if in_code or page == "SUMMARY.md":
            out.append(line)
            continue
        def fix(m):
            label, target = m.group(1), m.group(2)
            if re.match(r"^[a-z]+:", target) or target.startswith("#"):
                return m.group(0)
            path, _, anchor = target.partition("#")
            resolved = os.path.normpath(os.path.join(os.path.dirname(src), path))
            if not os.path.exists(resolved):
                errors.append(f"{page}: broken link {target}")
                return m.group(0)
            rel_docs = os.path.relpath(resolved, docs)
            if not rel_docs.startswith("..") and rel_docs in pages:
                return m.group(0)
            rel = os.path.relpath(resolved, root)
            kind = "tree" if os.path.isdir(resolved) else "blob"
            url = f"{repo}/{kind}/main/{rel}" + (f"#{anchor}" if anchor else "")
            return f"[{label}]({url})"
        out.append(link.sub(fix, line))
    open(os.path.join(stage, page), "w").write("\n".join(out))
if errors:
    print("\n".join(errors), file=sys.stderr)
    sys.exit(1)
print(f"{len(pages)} pages, links checked")
PY
mdbook build "$STAGE" -d "$(pwd)/target/book"
echo "site: $(pwd)/target/book/index.html"
