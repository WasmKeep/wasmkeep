"""Light local Markdown link checks and changed-file publication path checks.

No site build or third-party dependencies. Remote links are not checked. Book
pages and changed Markdown files are checked locally, anchors included: a link
to `page.md#section` or `#section` must name a heading of that page (as mdBook
makes its id) or an explicit `id`.
"""
import pathlib
import re
import runpy
import sys

root = pathlib.Path(__file__).resolve().parent.parent
scanner = runpy.run_path(str(root / 'scripts/check-benchmark-paths.py'))
errors = []
changed = {pathlib.Path(p) for p in sys.argv[1:]}
summary = root / 'docs/SUMMARY.md'
pages = {pathlib.Path('docs/SUMMARY.md')}
if summary.exists():
    pages.update(pathlib.Path('docs') / p for p in re.findall(r'\]\(([^)#]+\.md)\)', summary.read_text()))
pages.update(p for p in changed if p.suffix == '.md' and (root / p).is_file())
for path in sorted(changed):
    source = root / path
    if path.parts[0] not in ('bench', 'docs') or not source.is_file():
        continue
    for n, line in enumerate(source.read_bytes().decode(errors='replace').splitlines(), 1):
        category = scanner['problem'](str(path), line, str(root))
        if category:
            errors.append(f'{path}:{n}: {category}; redact before publishing')
anchor_cache = {}


def anchors(page):
    """Heading ids of a Markdown page, as mdBook makes them, and explicit ids."""
    if page in anchor_cache:
        return anchor_cache[page]
    ids, seen, fence = set(), {}, False
    for line in page.read_text(errors='replace').splitlines():
        if line.lstrip().startswith(('```', '~~~')):
            fence = not fence
            continue
        if fence:
            continue
        ids.update(re.findall(r'\bid="([^"]+)"', line))
        heading = re.match(r'^#{1,6}\s+(.*?)\s*#*\s*$', line)
        if not heading:
            continue
        text = re.sub(r'\[([^\]]*)\]\([^)]*\)', r'\1', heading.group(1))
        text = re.sub(r'<[^>]+>', '', text).replace('`', '')
        slug = ''.join(
            c.lower() if c.isalnum() or c in '_-' else '-' if c.isspace() else ''
            for c in text.replace('*', '')
        )
        n = seen.get(slug, 0)
        seen[slug] = n + 1
        ids.add(slug if n == 0 else f'{slug}-{n}')
    anchor_cache[page] = ids
    return ids


for path in sorted(pages):
    source = root / path
    if not source.is_file():
        errors.append(f'{path}: missing documentation page')
        continue
    fence = False
    for n, line in enumerate(source.read_text().splitlines(), 1):
        if line.lstrip().startswith(('```', '~~~')):
            fence = not fence
            continue
        if fence:
            continue
        for target in re.findall(r'(?<!!)\[[^\]]*\]\(([^)\s]+)\)', line):
            if re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:', target):
                continue
            target, _, anchor = target.partition('#')
            page = source if not target else source.parent / target
            if not page.exists():
                errors.append(f'{path}:{n}: broken local link')
            elif anchor and page.suffix == '.md' and anchor not in anchors(page):
                errors.append(f'{path}:{n}: no heading "#{anchor}" in {target or path.name}')
for error in errors:
    print(error, file=sys.stderr)
print(f'Light documentation checks: {len(pages)} pages, {len(errors)} problems.')
sys.exit(bool(errors))
