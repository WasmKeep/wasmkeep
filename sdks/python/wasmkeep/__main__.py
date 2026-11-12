"""`python -m wasmkeep build APP -o actor.wasm`: builds a module into a
WasmKeep actor, a WebAssembly component with CPython embedded.

It runs componentize-py on the `actor` world of the WIT files this package
carries (wasmkeep/wit), with this package on the module path, so an
installed `wasmkeep` is all a build needs: no clone, no WIT directory to
name. componentize-py is the one installed with this package (its pinned
requirement); from a source checkout without it, the `componentize-py`
command on PATH.
"""

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys

# Not `from . import ...`: the versions are read without touching the API.
from wasmkeep import WIT_VERSION, __version__

_PACKAGE = os.path.dirname(os.path.abspath(__file__))
# The version pyproject.toml requires (test_package.py compares the two).
COMPONENTIZE_PY = "0.25.1"


def _componentize_py():
    """The command that runs componentize-py: this interpreter's, else PATH's."""
    if importlib.util.find_spec("componentize_py") is not None:
        return [sys.executable, "-c", "import sys, componentize_py; sys.exit(componentize_py.script())"]
    found = shutil.which("componentize-py")
    if found is None:
        sys.exit(f"wasmkeep: componentize-py is not installed: pip install componentize-py=={COMPONENTIZE_PY}")
    # Another version generates other bindings than the ones this SDK imports.
    version = subprocess.run([found, "--version"], capture_output=True, text=True).stdout.split()
    if COMPONENTIZE_PY not in version:
        sys.exit(
            f"wasmkeep: {found} is {' '.join(version) or 'an unknown version'}; "
            f"this SDK needs componentize-py {COMPONENTIZE_PY}"
        )
    return [found]


def _build(args):
    wit = os.path.join(_PACKAGE, "wit")
    if not os.path.isfile(os.path.join(wit, "actor.wit")):
        sys.exit(f"wasmkeep: no WIT files in {wit}: the package is incomplete")
    if args.app.endswith(".py") or os.sep in args.app or "/" in args.app:
        sys.exit(f"wasmkeep: APP is a module name, not a file: `app` for app.py (got {args.app})")
    # The current directory first, then the other directories named, then
    # the one this package is in.
    paths = ["."] + (args.python_path or [])
    top = args.app.split(".")[0]
    if not any(
        os.path.isfile(os.path.join(path, top + ".py")) or os.path.isdir(os.path.join(path, top)) for path in paths
    ):
        sys.exit(f"wasmkeep: no module {top} in {', '.join(os.path.abspath(path) for path in paths)}")
    command = _componentize_py() + ["-d", wit, "-w", "actor", "componentize", args.app]
    for path in paths:
        command += ["-p", path]
    command += ["-p", os.path.dirname(_PACKAGE), "-o", args.output]
    code = subprocess.call(command)
    if code == 0:
        print(f"wrote {args.output} ({os.path.getsize(args.output)} bytes)")
    return code


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog="python -m wasmkeep",
        description="Build a Python module into a WasmKeep actor (a WebAssembly component).",
    )
    parser.add_argument(
        "--version", action="version", version=f"wasmkeep {__version__} (wasmkeep:actor@{WIT_VERSION})"
    )
    commands = parser.add_subparsers(dest="command", required=True, metavar="build")
    build = commands.add_parser(
        "build",
        help="build APP into a component",
        description="Build the module APP into a component with componentize-py, "
        "against the WIT world this package carries.",
    )
    build.add_argument("app", metavar="APP", help="the module that exports the actor: `app` for app.py")
    build.add_argument("-o", "--output", default="actor.wasm", help="the component to write (default: actor.wasm)")
    build.add_argument(
        "-p",
        "--python-path",
        action="append",
        metavar="DIR",
        help="another directory to find APP and what it imports in, besides the current one; repeatable",
    )
    build.set_defaults(run=_build)
    args = parser.parse_args(argv)
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
