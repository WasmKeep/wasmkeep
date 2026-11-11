"""One version: every SDK manifest carries the workspace version.

The workspace version is `[workspace.package] version` in Cargo.toml (the
server's, and every workspace crate's). Each entry of MANIFESTS names a file
and where its version is; a file that is missing, has no version there, or
has another version fails the check. Add a manifest as one line.
"""
import json
import pathlib
import sys

try:
    import tomllib
except ModuleNotFoundError:
    sys.exit('check-versions.py needs Python 3.11 or later (tomllib)')

root = pathlib.Path(__file__).resolve().parent.parent

# (file, format, path to the version inside it)
MANIFESTS = [
    ('sdks/typescript/package.json', 'json', ('version',)),
    ('sdks/typescript/package-lock.json', 'json', ('version',)),
    ('sdks/typescript/package-lock.json', 'json', ('packages', '', 'version')),
    ('sdks/python/pyproject.toml', 'toml', ('project', 'version')),
    ('examples/ts-counter/package-lock.json', 'json', ('packages', '../../sdks/typescript', 'version')),
    ('examples/ts-chat-room/package-lock.json', 'json', ('packages', '../../sdks/typescript', 'version')),
    ('examples/ts-jobs/package-lock.json', 'json', ('packages', '../../sdks/typescript', 'version')),
    ('examples/ts-workflows/package-lock.json', 'json', ('packages', '../../sdks/typescript', 'version')),
]


def load(path, kind):
    if kind == 'json':
        return json.loads(path.read_text())
    if kind == 'toml':
        return tomllib.loads(path.read_text())
    raise ValueError(f'unknown format {kind}')


def lookup(data, keys):
    for key in keys:
        if not isinstance(data, dict) or key not in data:
            return None
        data = data[key]
    return data if isinstance(data, str) else None


def name(keys):
    return '.'.join(key or '""' for key in keys)


try:
    expected = lookup(load(root / 'Cargo.toml', 'toml'), ('workspace', 'package', 'version'))
except (OSError, ValueError) as error:
    sys.exit(f'Cargo.toml: cannot read the workspace version: {error}')
if not expected:
    sys.exit('Cargo.toml: no [workspace.package] version')
errors = []
for file, kind, keys in MANIFESTS:
    try:
        found = lookup(load(root / file, kind), keys)
    except (OSError, ValueError) as error:
        errors.append(f'{file}: cannot read: {error}')
        continue
    if found is None:
        errors.append(f'{file}: no version at {name(keys)}')
    elif found != expected:
        errors.append(f'{file}: {name(keys)} is {found}, the workspace version is {expected}')
if errors:
    print('\n'.join(errors), file=sys.stderr)
    print('Every SDK manifest carries the version in Cargo.toml: change them together.', file=sys.stderr)
    sys.exit(1)
