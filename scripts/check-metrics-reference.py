#!/usr/bin/env python3
"""Fails when docs/reference/metrics.md and the server's metric names differ.

Names come from the string literals of crates/wasmkeep-server/src/metrics.rs
that start with wasmkeep_ (before its tests, comments left out). The page
lists each metric as `name` at the start of a table row.
"""
import pathlib
import re
import sys

root = pathlib.Path(__file__).resolve().parent.parent
source = (root / 'crates/wasmkeep-server/src/metrics.rs').read_text()
source = source.split('\n#[cfg(test)]\nmod tests')[0]
source = '\n'.join(l for l in source.splitlines() if not l.lstrip().startswith('//'))
code = set(re.findall(r'"(wasmkeep_[a-z0-9_]+)', source))
page = (root / 'docs/reference/metrics.md').read_text()
docs = set(re.findall(r'^\| `(wasmkeep_[a-z0-9_]+)` \|', page, re.M))
missing, extra = sorted(code - docs), sorted(docs - code)
for n in missing:
    print(f'in metrics.rs, not in docs/reference/metrics.md: {n}', file=sys.stderr)
for n in extra:
    print(f'in docs/reference/metrics.md, not in metrics.rs: {n}', file=sys.stderr)
if missing or extra:
    sys.exit(1)
print(f'metrics reference matches the code: {len(code)} metrics')
