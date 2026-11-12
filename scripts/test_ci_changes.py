"""Tests for scripts/ci-changes.sh (run by scripts/check.sh), on disposable repositories."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parent / 'ci-changes.sh'
BASE = {
    'README.md': 'readme\n',
    'docs/overview.md': 'overview\n' * 20,
    'docs/reference/api.md': 'api\n',
    'bench/results/2026-01-01-abc.csv': 'a,b\n',
    'crates/wasmkeep-server/src/lib.rs': 'pub fn f() {}\n' * 20,
    '.github/workflows/ci.yml': 'name: CI\n',
    '.github/workflows/release.yml': 'name: release\n',
    '.github/workflows/publish.yml': 'name: publish\n',
    'deploy/compose/README.md': 'compose\n',
    'deploy/systemd/units/README.md': 'units\n',
    'sdks/go/README.md': 'go\n',
    'examples/jobs/app/README.md': 'jobs\n',
}


class Changes(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        # No configuration but the fixture's own: no signing, no hooks.
        self.env = dict(os.environ, GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1')
        self.git('init', '-q', '-b', 'main')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.invalid')
        for path, content in BASE.items():
            self.write(path, content)
        self.commit('base')

    def git(self, *args):
        return subprocess.run(('git',) + args, cwd=self.root, env=self.env, check=True,
                              text=True, capture_output=True).stdout

    def write(self, path, content='changed\n'):
        path = self.root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def commit(self, message):
        self.git('add', '-A')
        self.git('commit', '-q', '--allow-empty', '-m', message)

    def answer(self, *args, cwd=None):
        # A pull request into main, unless the case names another base.
        if args == ('pull_request',):
            args += ('main',)
        result = subprocess.run([str(SCRIPT), *args], cwd=cwd or self.root, env=self.env,
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn(result.stdout, ('code=true\n', 'code=false\n'))
        return result.stdout == 'code=true\n'

    def code(self, change=None, event='pull_request'):
        """The answer for a pull request making `change`, checked out as CI
        checks it out: merged into main, two commits deep."""
        self.git('switch', '-q', '-c', 'change', 'main')
        if change:
            change()
        self.commit('change')
        self.git('switch', '-q', '--detach', 'main')
        self.git('merge', '-q', '--no-ff', '-m', 'merge', 'change')
        self.git('branch', '-q', '-D', 'change')
        return self.answer(event)

    def writes(self, *paths):
        return lambda: [self.write(path) for path in paths]

    def test_docs_only(self):
        self.assertFalse(self.code(self.writes(
            'README.md', 'ROADMAP.md', 'CHANGELOG.md', 'CONTRIBUTING.md', 'CLAUDE.md',
            'docs/overview.md', 'docs/adr/0001-plain-cargo.md', 'docs/book.toml',
            'deploy/compose/README.md', 'deploy/systemd/units/README.md',
            'sdks/go/README.md', 'examples/jobs/app/README.md')))

    def test_a_new_readme_beside_code_is_code(self):
        # A new example or a generated directory: a build globs over the
        # first and regenerates the second, so a README alone can break it.
        for path in ('examples/ts-new/README.md', 'sdks/go/internal/gen/README.md',
                     'sdks/typescript/wit/README.md', 'deploy/new/README.md'):
            with self.subTest(path=path):
                self.assertTrue(self.code(self.writes(path)))

    def test_a_pull_request_into_another_branch_runs_everything(self):
        # Its base may hold code main never tested; retargeting it to main
        # starts no new run.
        self.git('switch', '-q', '-c', 'change', 'main')
        self.write('docs/overview.md')
        self.commit('change')
        self.git('switch', '-q', '--detach', 'main')
        self.git('merge', '-q', '--no-ff', '-m', 'merge', 'change')
        self.assertFalse(self.answer('pull_request', 'main'))
        self.assertTrue(self.answer('pull_request', 'feature-x'))
        self.assertTrue(self.answer('pull_request', ''))

    def test_results_only(self):
        self.assertFalse(self.code(self.writes(
            'bench/results/2026-02-02-def.csv', 'bench/results/soak/run.json')))

    def test_a_doc_and_a_rust_file(self):
        self.assertTrue(self.code(self.writes('docs/overview.md', 'crates/wasmkeep-server/src/lib.rs')))

    def test_the_workflow_alone(self):
        self.assertTrue(self.code(self.writes('.github/workflows/ci.yml')))

    def test_the_release_workflows_alone(self):
        self.assertFalse(self.code(self.writes('.github/workflows/release.yml', '.github/workflows/publish.yml')))

    def test_a_release_workflow_and_the_ci_workflow(self):
        self.assertTrue(self.code(self.writes('.github/workflows/publish.yml', '.github/workflows/ci.yml')))

    def test_another_workflow_is_code(self):
        self.assertTrue(self.code(self.writes('.github/workflows/nightly.yml')))
        self.assertTrue(self.code(self.writes('.github/workflows/sub/publish.yml')))

    def test_this_script_alone(self):
        self.assertTrue(self.code(self.writes('scripts/ci-changes.sh')))

    def test_the_generated_api_reference(self):
        self.assertTrue(self.code(self.writes('docs/reference/api.md')))

    def test_files_that_only_look_like_documentation(self):
        for path in ('docs/diagram.svg', 'docs/theme/book.toml', 'deploy/grafana/wasmkeep.json',
                     'deploy/README.md', 'bench/run.sh', 'bench/compare/README.md',
                     'bench/results.md', 'CLA.md', 'LICENSE', 'sdks/README.md',
                     'crates/wasmkeep-server/README.md', 'readme.md', 'x/README.md',
                     'xdocs/a.md', 'x/docs/a.md', 'docs/a.md.rs'):
            with self.subTest(path=path):
                self.assertTrue(self.code(self.writes(path)))

    def test_an_unknown_top_level_file(self):
        self.assertTrue(self.code(self.writes('NEW.md')))
        self.assertTrue(self.code(self.writes('Makefile')))

    def test_code_renamed_into_the_docs(self):
        self.assertTrue(self.code(
            lambda: self.git('mv', 'crates/wasmkeep-server/src/lib.rs', 'docs/lib.md')))

    def test_a_doc_renamed_out_of_the_docs(self):
        self.assertTrue(self.code(lambda: self.git('mv', 'docs/overview.md', 'crates/overview.md')))

    def test_a_doc_renamed_to_a_doc(self):
        self.assertFalse(self.code(lambda: self.git('mv', 'docs/overview.md', 'docs/reference/start.md')))

    def test_a_deleted_doc(self):
        self.assertFalse(self.code(lambda: self.git('rm', '-q', 'docs/overview.md')))

    def test_a_deleted_rust_file(self):
        self.assertTrue(self.code(lambda: self.git('rm', '-q', 'crates/wasmkeep-server/src/lib.rs')))

    def test_a_doc_with_spaces(self):
        self.assertFalse(self.code(self.writes('docs/getting started.md', 'bench/results/run 1/a b.csv')))

    def test_code_with_spaces(self):
        self.assertTrue(self.code(self.writes('docs/overview.md', 'crates/a b/src/my lib.rs')))

    def test_a_path_git_quotes(self):
        # Quoted, so it matches nothing: code, whatever the directory and
        # whatever the checkout's own core.quotePath.
        self.assertTrue(self.code(self.writes('docs/résumé.md')))
        self.git('config', 'core.quotePath', 'false')
        self.assertTrue(self.code(self.writes('docs/café.md')))

    def test_an_empty_diff(self):
        self.assertTrue(self.code())

    def test_a_push(self):
        self.assertTrue(self.code(self.writes('docs/overview.md'), event='push'))

    def test_any_other_event(self):
        self.assertTrue(self.code(self.writes('docs/overview.md'), event='merge_group'))
        self.assertTrue(self.answer())
        self.assertTrue(self.answer(''))

    def test_a_checkout_that_is_not_a_merge(self):
        self.write('docs/overview.md')
        self.commit('change')
        self.assertTrue(self.answer('pull_request'))

    def test_a_shallow_checkout_without_the_base(self):
        self.code(self.writes('docs/overview.md'))
        with tempfile.TemporaryDirectory() as clone:
            self.git('clone', '-q', '--depth', '1', self.root.as_uri(), clone)
            self.assertTrue(self.answer('pull_request', cwd=clone))
            self.git('-C', clone, 'fetch', '-q', '--depth', '2')
            self.assertFalse(self.answer('pull_request', cwd=clone))

    def test_outside_a_repository(self):
        with tempfile.TemporaryDirectory() as empty:
            self.env['GIT_CEILING_DIRECTORIES'] = str(Path(empty).parent)
            self.assertTrue(self.answer('pull_request', cwd=empty))


if __name__ == '__main__':
    unittest.main()
