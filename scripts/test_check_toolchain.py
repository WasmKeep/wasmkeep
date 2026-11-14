"""Tests for scripts/check-toolchain.py: the toolchain image pins what CI installs."""
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
FILES = ('deploy/toolchain/Dockerfile', 'deploy/Dockerfile', '.github/workflows/ci.yml')


class Toolchain(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        for f in FILES:
            (self.root / f).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(ROOT / f, self.root / f)

    def run_check(self):
        return subprocess.run([sys.executable, '-I', str(ROOT / 'scripts/check-toolchain.py'), str(self.root)],
                              capture_output=True, text=True)

    def edit(self, file, old, new):
        path = self.root / file
        text = path.read_text()
        self.assertIn(old, text)
        path.write_text(text.replace(old, new))

    def test_the_repository_passes(self):
        result = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_a_pin_moved_in_ci_only_fails(self):
        self.edit('.github/workflows/ci.yml', 'componentize-py==', 'componentize-py==9')
        result = self.run_check()
        self.assertEqual(result.returncode, 1)
        self.assertIn('COMPONENTIZE_PY_VERSION', result.stderr)

    def test_a_pin_missing_from_the_image_fails(self):
        self.edit('deploy/toolchain/Dockerfile', 'ARG TINYGO_VERSION=', 'ARG TINYGO_RELEASE=')
        result = self.run_check()
        self.assertEqual(result.returncode, 1)
        self.assertIn('no ARG TINYGO_VERSION', result.stderr)

    def test_another_node_major_or_base_image_fails(self):
        self.edit('deploy/toolchain/Dockerfile', 'ARG NODE_VERSION=22.', 'ARG NODE_VERSION=24.')
        self.edit('deploy/Dockerfile', 'ARG RUST_IMAGE=docker.io/library/rust:', 'ARG RUST_IMAGE=docker.io/library/rust:0')
        stderr = self.run_check().stderr
        self.assertIn('NODE_VERSION', stderr)
        self.assertIn('RUST_IMAGE', stderr)


if __name__ == '__main__':
    unittest.main()
