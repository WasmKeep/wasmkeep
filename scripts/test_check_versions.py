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
        self.python('1.2.3')
        self.header('1.2.3')
        self.zig('1.2.3')

    def header(self, version):
        # Another string constant comes first: only the SDK's version counts.
        path = self.root / 'sdks/c/wasmkeep.h'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f'#ifndef WASMKEEP_H\n#define OTHER "9.9.9"\n#define WASMKEEP_SDK_VERSION "{version}"\n#endif\n')

    def zig(self, version):
        path = self.root / 'sdks/zig/wasmkeep.zig'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f'const std = @import("std");\npub const other = "9.9.9";\npub const version = "{version}";\n')

    def python(self, version):
        path = self.root / 'sdks/python/pyproject.toml'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f'[project]\nname = "sdk"\nversion = "{version}"\n')

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

    def test_another_python_package_version_fails(self):
        self.python('1.2.2')
        self.fails('sdks/python/pyproject.toml: project.version is 1.2.2')

    def test_another_c_header_version_fails(self):
        self.header('1.2.4')
        self.fails('sdks/c/wasmkeep.h: WASMKEEP_SDK_VERSION is 1.2.4')

    def test_a_c_header_without_the_version_fails(self):
        (self.root / 'sdks/c/wasmkeep.h').write_text('#define OTHER "1.2.3"\n')
        self.fails('sdks/c/wasmkeep.h: no version at WASMKEEP_SDK_VERSION')

    def test_another_zig_version_fails(self):
        self.zig('1.2.4')
        self.fails('sdks/zig/wasmkeep.zig: version is 1.2.4')

    def test_a_registry_readme_links_at_the_release_tag(self):
        readme = self.root / 'sdks/python/README.md'
        readme.write_text('[docs](https://github.com/WasmKeep/wasmkeep/blob/v1.2.3/docs/x.md)\n')
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)
        readme.write_text('[docs](https://github.com/WasmKeep/wasmkeep/blob/main/docs/x.md)\n')
        self.fails('sdks/python/README.md: a link at main, the release is v1.2.3')

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
