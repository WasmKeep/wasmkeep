"""Tests for scripts/check-versions.py: SDK manifests carry the workspace version."""
import json
import pathlib
import shutil
import subprocess
import tempfile
import unittest

SCRIPTS = pathlib.Path(__file__).resolve().parent
PACKAGE = 'sdks/typescript/package.json'
LOCK = 'sdks/typescript/package-lock.json'
EXAMPLES = ('ts-counter', 'ts-chat-room', 'ts-jobs', 'ts-workflows')


class Versions(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        (self.root / 'scripts').mkdir()
        shutil.copy2(SCRIPTS / 'check-versions.py', self.root / 'scripts/check-versions.py')
        (self.root / 'sdks/typescript').mkdir(parents=True)
        self.workspace('1.2.3')
        self.package('1.2.3')
        self.lock('1.2.3', '1.2.3')
        for example in EXAMPLES:
            self.example(example, '1.2.3')

    def example(self, name, linked):
        # An example's lockfile records the SDK it links by path.
        path = self.root / 'examples' / name / 'package-lock.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({'packages': {'../../sdks/typescript': {'version': linked}}}))

    def workspace(self, version):
        # A dependency's version comes first: only [workspace.package] counts.
        (self.root / 'Cargo.toml').write_text(
            '[workspace.dependencies]\nother = { version = "9.9.9" }\n\n'
            f'[workspace.package]\nversion = "{version}"\nedition = "2024"\n'
        )

    def package(self, version):
        (self.root / PACKAGE).write_text(json.dumps({'name': 'sdk', 'version': version}))

    def lock(self, top, package):
        (self.root / LOCK).write_text(json.dumps({
            'name': 'sdk', 'version': top,
            'packages': {'': {'name': 'sdk', 'version': package}, 'node_modules/x': {'version': '4.5.6'}},
        }))

    def check(self):
        return subprocess.run(
            ['python3', '-I', 'scripts/check-versions.py'],
            cwd=self.root, capture_output=True, text=True,
        )

    def fails(self, *expected):
        result = self.check()
        self.assertEqual(result.returncode, 1, result.stderr)
        for text in expected:
            self.assertIn(text, result.stderr)

    def test_the_same_version_everywhere_passes(self):
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, '')

    def test_another_package_version_fails(self):
        self.package('1.2.4')
        self.fails(f'{PACKAGE}: version is 1.2.4, the workspace version is 1.2.3')

    def test_another_lockfile_version_fails(self):
        self.lock('0.0.0', '1.2.3')
        self.fails(f'{LOCK}: version is 0.0.0')

    def test_another_lockfile_root_package_version_fails(self):
        self.lock('1.2.3', '0.0.0')
        self.fails(f'{LOCK}: packages."".version is 0.0.0')

    def test_an_example_lockfile_with_another_sdk_version_fails(self):
        self.example('ts-jobs', '0.0.0')
        self.fails('examples/ts-jobs/package-lock.json: packages.../../sdks/typescript.version is 0.0.0')

    def test_a_workspace_bump_alone_fails_for_every_manifest(self):
        self.workspace('1.3.0')
        self.fails(PACKAGE, f'{LOCK}: version is', f'{LOCK}: packages."".version is')

    def test_a_missing_manifest_fails(self):
        (self.root / PACKAGE).unlink()
        self.fails(f'{PACKAGE}: cannot read')

    def test_a_manifest_without_a_version_fails(self):
        (self.root / PACKAGE).write_text(json.dumps({'name': 'sdk'}))
        self.fails(f'{PACKAGE}: no version at version')

    def test_a_missing_workspace_version_fails(self):
        (self.root / 'Cargo.toml').write_text('[workspace]\nmembers = []\n')
        result = self.check()
        self.assertEqual(result.returncode, 1)
        self.assertIn('no [workspace.package] version', result.stderr)


if __name__ == '__main__':
    unittest.main()
