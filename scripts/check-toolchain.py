"""The toolchain image and the release build with the tools CI builds the examples with.

deploy/toolchain/Dockerfile pins each tool with an ARG; each must equal
what .github/workflows/ci.yml installs (Node.js: the same major version,
since CI takes the newest of it), and the base image must be
deploy/Dockerfile's. A pin missing on either side fails too, so moving one
in ci.yml without the image (or the other way round) is caught here.
.github/workflows/release.yml builds the counter of every language the
image is for (the release's counter-<lang>.wasm) and must pin the same
versions too. The Python tools (componentize-py, ziglang) are pinned with
hashes in scripts/requirements/languages.txt, which both workflows install.

    python3 -I scripts/check-toolchain.py [ROOT]
"""
import pathlib
import re
import sys

# ARG in the Dockerfile -> a pattern whose group 1 is the version in ci.yml.
PINS = {
    'WASM_TOOLS_VERSION': r'wasm-tools@([0-9][^,\s]*)',
    'WIT_BINDGEN_VERSION': r'wit-bindgen-cli@([0-9][^,\s]*)',
    'GO_VERSION': r'go-version: "([0-9.]+)"',
    'TINYGO_VERSION': r'tinygo/releases/download/v([0-9.]+)/',
    'WIT_BINDGEN_GO_VERSION': r'wit-bindgen-go@v([0-9.]+)',
    'COMPONENTIZE_PY_VERSION': r'componentize-py==([0-9.]+)',
    'ZIGLANG_VERSION': r'ziglang==([0-9.]+)',
}
# Tools the release's counters do not need (their builds do not run them):
# checked only when release.yml names them.
OPTIONAL_IN_RELEASE = {'WIT_BINDGEN_GO_VERSION'}


def args(text):
    return dict(re.findall(r'^ARG ([A-Z0-9_]+)=(\S+)$', text, re.M))


def check(root):
    root = pathlib.Path(root)
    image = args((root / 'deploy/toolchain/Dockerfile').read_text())
    server = args((root / 'deploy/Dockerfile').read_text())
    ci = (root / '.github/workflows/ci.yml').read_text()
    release = (root / '.github/workflows/release.yml').read_text()
    # The Python tools are installed from a hashed requirements file: its
    # pins count for a workflow that installs it.
    req_path = 'scripts/requirements/languages.txt'
    req = (root / req_path).read_text()
    if req_path in ci:
        ci += req
    if req_path in release:
        release += req
    errors = []
    for arg, pattern in PINS.items():
        found = sorted(set(re.findall(pattern, release)))
        if arg in OPTIONAL_IN_RELEASE and not found:
            continue
        if arg in image and found != [image[arg]]:
            errors.append(f'{arg} is {image[arg]} in deploy/toolchain/Dockerfile, '
                          f'{", ".join(found) or "missing"} in release.yml')
        found = sorted(set(re.findall(pattern, ci)))
        if arg not in image:
            errors.append(f'deploy/toolchain/Dockerfile: no ARG {arg}')
        elif not found:
            errors.append(f'.github/workflows/ci.yml: no version for {arg} ({pattern})')
        elif found != [image[arg]]:
            errors.append(f'{arg} is {image[arg]} in deploy/toolchain/Dockerfile, {", ".join(found)} in ci.yml')
    node = sorted(set(re.findall(r'node-version: "?([0-9]+)"?', ci)))
    if sorted(set(re.findall(r'node-version: "?([0-9]+)"?', release))) != node:
        errors.append('node-version: release.yml and ci.yml must use the same Node.js major')
    if 'NODE_VERSION' not in image or not node:
        errors.append('NODE_VERSION: missing in deploy/toolchain/Dockerfile or node-version in ci.yml')
    elif node != [image['NODE_VERSION'].split('.')[0]]:
        errors.append(f'NODE_VERSION is {image["NODE_VERSION"]}, ci.yml uses Node.js {", ".join(node)}')
    if image.get('RUST_IMAGE') != server.get('RUST_IMAGE') or 'RUST_IMAGE' not in image:
        errors.append('RUST_IMAGE: deploy/toolchain/Dockerfile and deploy/Dockerfile must name the same base')
    return errors


if __name__ == '__main__':
    default = pathlib.Path(__file__).resolve().parent.parent
    errors = check(sys.argv[1] if len(sys.argv) > 1 else default)
    if errors:
        print('\n'.join(errors), file=sys.stderr)
        print('The toolchain image pins what CI uses: change them together.', file=sys.stderr)
        sys.exit(1)
