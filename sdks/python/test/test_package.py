"""The numbers the package states about itself: `__version__` is the version
in pyproject.toml, and `WIT_VERSION` is the version of the WIT package the
wheel carries in wasmkeep/wit."""

import os
import re
import unittest

import host_stub  # noqa: F401 - before wasmkeep

import wasmkeep

_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def _find(path, pattern):
    with open(os.path.join(_ROOT, path), encoding="utf-8") as f:
        found = re.search(pattern, f.read(), re.MULTILINE)
    assert found, f"{path} has no line matching {pattern}"
    return found.group(1)


class Versions(unittest.TestCase):
    def test_version_is_the_one_in_pyproject(self):
        self.assertEqual(wasmkeep.__version__, _find("pyproject.toml", r'^version = "([^"]+)"$'))

    def test_the_build_names_the_componentize_py_pyproject_requires(self):
        from wasmkeep.__main__ import COMPONENTIZE_PY

        self.assertEqual(COMPONENTIZE_PY, _find("pyproject.toml", r'"componentize-py==([^"]+)"'))

    def test_wit_version_is_the_bundled_package(self):
        self.assertEqual(
            wasmkeep.WIT_VERSION,
            _find("wasmkeep/wit/actor.wit", r"^package wasmkeep:actor@([^;]+);$"),
        )


if __name__ == "__main__":
    unittest.main()
