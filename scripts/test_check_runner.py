"""Exercise the real runner with disposable repositories and command stubs."""
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent


class Runner(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'scripts').mkdir()
        for name in ('check.sh', 'doctor.sh', 'release-notes.sh'):
            shutil.copy2(SCRIPTS / name, self.root / 'scripts' / name)
        self.write('CHANGELOG.md', '# Changelog\n\n## 1.2.3\n\nNotes.\n')
        for name in ('docs-site.sh', 'licenses.sh', 's3-suites.sh', 'languages.sh',
                     'package-rust-sdk.sh'):
            self.write('scripts/' + name, '#!/bin/sh\necho "' + name + '" >> "$CALLS"\n', executable=True)
        for name in ('test_check_benchmark_paths.py', 'test_check_runner.py'):
            self.write('scripts/' + name, '')
        self.write('Cargo.toml', '[workspace.package]\nversion = "1.2.3"\nrust-version = "1.97"\n')
        self.write('crates/wasmkeep-server/Cargo.toml', '[package]\nname = "wasmkeep-server"\n')
        self.write('sim/Cargo.toml', '[package]\nname = "wasmkeep-sim"\n')
        self.write('sdks/python/test/run.sh', '#!/bin/sh\necho "python-sdk-tests" >> "$CALLS"\n', executable=True)
        self.write('sdks/c/test/run.sh', '#!/bin/sh\necho "c-sdk-tests" >> "$CALLS"\n', executable=True)
        self.write('sdks/go/go.mod', '')
        self.write('sdks/typescript/node_modules/.keep', '')
        self.bin = self.root / 'bin'
        self.bin.mkdir()
        stub = '''#!/bin/bash
name=${0##*/}
printf '%s %s flags=%s seeds=%s\\n' "$name" "$*" "${RUSTFLAGS:-}" "${WASMKEEP_SIM_SEEDS:-}" >> "$CALLS"
if [[ -n "${FAIL_MATCH:-}" && "$name $*" == *"$FAIL_MATCH"* ]]; then exit 9; fi
case "$name $*" in
  'rustc --print target-libdir --target wasm32-wasip2') echo \"$TARGET_LIBDIR\" ;;
  'rustup toolchain list') echo 1.97.0-x86_64-unknown-linux-gnu ;;
esac
'''
        for name in ('cargo', 'rustup', 'rustc', 'mdbook', 'python3', 'go', 'npm'):
            self.write('bin/' + name, stub, executable=True)
        self.env = dict(os.environ, PATH=str(self.bin) + os.pathsep + os.environ['PATH'], CALLS=str(self.root / 'calls'))
        self.write('target-libs/libcore-test.rlib', '')
        self.env['TARGET_LIBDIR'] = str(self.root / 'target-libs')
        self.env.pop('CHECK_BASE', None)
        self.env.pop('FAIL_MATCH', None)
        self.git('init', '-q')
        self.git('config', 'user.name', 'Test')
        self.git('config', 'user.email', 'test@example.invalid')
        self.git('add', '.')
        self.git('-c', 'commit.gpgsign=false', 'commit', '-qm', 'fixture')
        self.git('update-ref', 'refs/remotes/origin/main', 'HEAD')

    def write(self, path, content, executable=False):
        path = self.root / path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        if executable:
            path.chmod(0o755)

    def git(self, *args):
        return subprocess.run(['git', *args], cwd=self.root, env=self.env, check=True, capture_output=True)

    def run_check(self, *args, fail=None):
        env = dict(self.env)
        if fail:
            env['FAIL_MATCH'] = fail
        result = subprocess.run(['bash', 'scripts/check.sh', *args], cwd=self.root, env=env, capture_output=True, text=True)
        calls = (self.root / 'calls').read_text() if (self.root / 'calls').exists() else ''
        return result, calls

    def test_docs_need_no_rust_or_site_builder(self):
        self.write('docs/note.md', 'A note\n')
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('check-benchmark-paths.py --range ', calls)
        self.assertIn('check-docs.py docs/note.md', calls)
        self.assertNotIn('cargo ', calls)
        self.assertNotIn('docs-site.sh', calls)

    def test_affected_crate_and_dependents(self):
        self.write('crates/wasmkeep-server/src/error.rs', '')
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('-E (rdeps(wasmkeep-server)) - binary(cluster_over_tls)', calls)
        self.assertIn('clippy --workspace --all-targets --locked -- -D warnings', calls)
        self.assertNotIn('cargo test ', calls)
        # What it did not run is listed.
        self.assertIn('the cluster suite over TLS', result.stdout)
        self.assertIn('the S3 suites', result.stdout)
        self.assertNotIn('s3-suites.sh', calls)

    def test_replication_runs_simulation(self):
        self.write('crates/wasmkeep-cluster/Cargo.toml', '[package]\nname = "wasmkeep-cluster"\n')
        self.write('crates/wasmkeep-cluster/src/replication.rs', '')
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('flags=--cfg madsim seeds=200', calls)

    def test_server_code_runs_no_simulation(self):
        # The simulations build wasmkeep-sim and wasmkeep-cluster only.
        self.write('crates/wasmkeep-server/src/actor.rs', '')
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('madsim', calls)

    def test_a_wit_copy_that_differs_fails(self):
        self.write('crates/wasmkeep-wit/wit/actor.wit', 'world actor {}\n')
        self.write('sdks/typescript/wit/actor.wit', 'world actor {}\n')
        result, _ = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.write('sdks/typescript/wit/actor.wit', 'world other {}\n')
        result, _ = self.run_check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('npm run sync', result.stderr)

    def test_a_rust_sdk_wit_copy_that_differs_fails(self):
        self.write('crates/wasmkeep-wit/wit/deps/io.wit', 'package wasi:io;\n')
        self.write('crates/wasmkeep-sdk/wit/deps/io.wit', 'package wasi:io;\n')
        result, _ = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.write('crates/wasmkeep-sdk/wit/deps/io.wit', 'package wasi:other;\n')
        result, _ = self.run_check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('crates/wasmkeep-sdk/wit differs', result.stderr)

    def test_a_version_without_a_changelog_section_fails(self):
        self.write('Cargo.toml', '[workspace.package]\nversion = "1.3.0"\nrust-version = "1.97"\n')
        result, _ = self.run_check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('CHANGELOG.md has no section for 1.3.0', result.stderr)
        self.write('CHANGELOG.md', '# Changelog\n\n## 1.3.0\n\nNew.\n\n## 1.2.3\n\nNotes.\n')
        result, _ = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_s3_runs_the_s3_suites(self):
        result, calls = self.run_check('--s3')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('s3-suites.sh', calls)
        self.assertNotIn('the S3 suites', result.stdout)

    def test_simulation_only_runs_real_simulation(self):
        self.write('sim/tests/example.rs', '')
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('cargo test -p wasmkeep-sim --locked --target-dir target/madsim', calls)
        self.assertNotIn('nextest run', calls)

    def test_full_leaves_the_retired_lease_sim_to_deep(self):
        self.write('sim/tests/madsim_lease.rs', '')
        self.write('sim/tests/madsim_node_lease.rs', '')
        self.git('add', '.')
        self.git('-c', 'commit.gpgsign=false', 'commit', '-qm', 'sims')
        self.git('update-ref', 'refs/remotes/origin/main', 'HEAD')
        result, calls = self.run_check('--full')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('--lib --test madsim_node_lease flags=--cfg madsim seeds=200', calls)
        self.assertNotIn('--test madsim_lease ', calls)
        self.assertIn('madsim_lease (the retired per-actor lease): --deep', result.stdout)

    def test_deep_runs_every_simulation_one_at_a_time(self):
        self.write('sim/tests/madsim_lease.rs', '')
        self.write('sim/tests/madsim_node_lease.rs', '')
        self.git('add', '.')
        self.git('-c', 'commit.gpgsign=false', 'commit', '-qm', 'sims')
        self.git('update-ref', 'refs/remotes/origin/main', 'HEAD')
        env = dict(self.env, CHECK_DEEP_SEEDS='50')
        result = subprocess.run(['bash', 'scripts/check.sh', '--deep'], cwd=self.root, env=env,
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        calls = (self.root / 'calls').read_text()
        for name in ('madsim_lease', 'madsim_node_lease'):
            self.assertIn('--release --target-dir target/madsim-release --test ' + name
                          + ' -- --nocapture --test-threads 1 flags=--cfg madsim seeds=50', calls)
        self.assertIn('deep: madsim_node_lease 50 seeds in', result.stdout)
        self.assertTrue(list((self.root / 'target' / 'deep').glob('*-50.log')))

    def test_script_only_runs_script_tests(self):
        self.write('scripts/change.py', '')
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('test_check_runner.py', calls)
        self.assertIn('test_check_benchmark_paths.py', calls)
        self.assertNotIn('cargo ', calls)

    def test_benchmark_shell_runs_preflight(self):
        self.write('bench/preflight.sh', '#!/bin/bash\ntrue\n')
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('nextest run -p wasmkeep-compare --test preflight --locked', calls)

    def test_shared_inputs_run_workspace(self):
        self.write('wit/new.wit', '')
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('nextest run --workspace --locked -E all() - binary(cluster_over_tls) flags=', calls)

    def test_full_has_all_local_gates(self):
        result, calls = self.run_check('--full')
        self.assertEqual(result.returncode, 0, result.stderr)
        for expected in ('test_check_runner.py', 'test_check_benchmark_paths.py',
                         'check-benchmark-paths.py --range ', 'flags=--cfg madsim seeds=200',
                         'cargo +1.97 check --workspace --all-targets --locked',
                         'cargo deny check', 'licenses.sh', 'docs-site.sh',
                         'npm test --silent', 'python-sdk-tests', 'c-sdk-tests', 'go vet ./...', 'go test ./...'):
            self.assertIn(expected, calls)
        # Everything, the cluster suite over TLS included; the S3 suites and
        # what the gate never covers are listed as not run.
        self.assertIn('nextest run --workspace --locked flags=', calls)
        self.assertIn('== not run', result.stdout)
        self.assertIn('the S3 suites', result.stdout)
        self.assertIn('real-bucket soak', result.stdout)
        self.assertIn('languages.sh', calls)
        self.assertNotIn('every language (CHECK_SKIP_LANGS=1)', result.stdout)

    def test_skipping_the_languages_is_listed(self):
        env = dict(self.env)
        env['CHECK_SKIP_LANGS'] = '1'
        result = subprocess.run(['bash', 'scripts/check.sh', '--full'], cwd=self.root, env=env, capture_output=True, text=True)
        calls = (self.root / 'calls').read_text()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn('languages.sh', calls)
        self.assertIn('every language (CHECK_SKIP_LANGS=1)', result.stdout)

    def test_sdk_change_runs_only_its_unit_tests(self):
        self.write('sdks/go/kv/kv.go', '')
        result, calls = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn('go test ./...', calls)
        self.assertNotIn('python-sdk-tests', calls)
        self.assertNotIn('c-sdk-tests', calls)
        self.assertNotIn('npm test', calls)
        # Examples build on the SDKs, so the Rust suite runs too.
        self.assertIn('nextest run --workspace --locked', calls)

    def test_go_wit_copy_must_match_the_host(self):
        self.write('crates/wasmkeep-wit/wit/actor.wit', 'world actor {}\n')
        self.write('sdks/go/wit/actor.wit', 'world actor {}\n')
        # The world the Go SDK adds for TinyGo is not part of the copy.
        self.write('sdks/go/wit/tinygo.wit', 'world actor-tinygo {}\n')
        result, _ = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)
        self.write('sdks/go/wit/actor.wit', 'world other {}\n')
        result, _ = self.run_check()
        self.assertEqual(result.returncode, 1)
        self.assertIn('sdks/go/wit differs', result.stderr)

    def test_failure_propagates(self):
        for failure in ('check-benchmark-paths.py', 'check-docs.py', 'test_check_runner.py',
                        'cargo clippy', 'cargo nextest run', 'cargo test -p wasmkeep-sim',
                        'cargo +1.97 check', 'cargo deny check', 'go test', 'npm test'):
            with self.subTest(failure=failure):
                result, _ = self.run_check('--full', fail=failure)
                self.assertNotEqual(result.returncode, 0)
                self.assertNotIn('selected checks passed', result.stdout)

    def test_missing_nextest_never_falls_back(self):
        self.write('sim/test.rs', '')
        result, calls = self.run_check(fail='cargo nextest --version')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr.count('missing: cargo-nextest'), 1)
        self.assertNotIn('cargo test ', calls)

    def test_system_compiler_target_without_rustup(self):
        self.write('bin/rustup', '#!/bin/sh\nexit 1\n', executable=True)
        self.write('crates/wasmkeep-server/src/error.rs', '')
        result, _ = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_missing_target_library_fails(self):
        (self.root / 'target-libs/libcore-test.rlib').unlink()
        self.write('crates/wasmkeep-server/src/error.rs', '')
        result, calls = self.run_check()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stderr.count('missing: wasm32-wasip2'), 1)
        self.assertNotIn('nextest run', calls)

    def test_missing_base_fails_closed(self):
        self.git('update-ref', '-d', 'refs/remotes/origin/main')
        result, _ = self.run_check()
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('Cannot determine check base', result.stderr)

    def test_explicit_base(self):
        self.git('update-ref', '-d', 'refs/remotes/origin/main')
        self.env['CHECK_BASE'] = 'HEAD'
        result, _ = self.run_check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_invalid_shell_fails(self):
        self.write('scripts/bad.sh', 'if\n')
        result, _ = self.run_check()
        self.assertNotEqual(result.returncode, 0)

    def test_doctor_reports_each_missing_prerequisite(self):
        # Commands fail without producing misleading installation side effects.
        for name in ('cargo', 'rustup', 'rustc'):
            self.write('bin/' + name, '#!/bin/sh\nexit 1\n', executable=True)
        (self.bin / 'mdbook').unlink()
        result = subprocess.run(['bash', 'scripts/doctor.sh'], cwd=self.root, env=self.env, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)
        for name in ('cargo-nextest', 'wasm32-wasip2', 'Rust 1.97', 'cargo-deny'):
            self.assertEqual(result.stderr.count('missing: ' + name), 1)


class LightDocs(unittest.TestCase):
    def test_links_and_private_untracked_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / 'scripts').mkdir()
            (root / 'docs').mkdir()
            for name in ('check-docs.py', 'check-benchmark-paths.py'):
                shutil.copy2(SCRIPTS / name, root / 'scripts' / name)
            (root / 'docs/SUMMARY.md').write_text('[Page](page.md)\n')
            page = root / 'docs/page.md'
            for text, expected in (('OK\n', 0), ('[bad](missing.md)\n', 1),
                                   ('Private: /home/example/private\n', 1)):
                page.write_text(text)
                result = subprocess.run([sys.executable, '-I', str(root / 'scripts/check-docs.py'), 'docs/page.md'], capture_output=True, text=True)
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertNotIn('/home/example/private', result.stderr)


if __name__ == '__main__':
    unittest.main()
