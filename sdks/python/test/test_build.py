"""`python -m wasmkeep` under plain CPython, without the bindings
componentize-py generates (no host_stub here: each case is a new process)."""

import os
import subprocess
import sys
import unittest

_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")


def _python(*args):
    # From the SDK's directory, so `wasmkeep` is this one; no bytecode written.
    return subprocess.run(
        [sys.executable, "-B", "-s", *args], cwd=_ROOT, capture_output=True, text=True, check=False
    )


class Outside(unittest.TestCase):
    def test_the_command_starts_without_the_bindings(self):
        done = _python("-m", "wasmkeep", "--version")
        self.assertEqual(done.returncode, 0, done.stderr)
        self.assertRegex(done.stdout, r"^wasmkeep \d+\.\d+\.\d+ \(wasmkeep:actor@\d+\.\d+\.\d+\)\n$")

    def test_build_needs_a_module(self):
        done = _python("-m", "wasmkeep", "build")
        self.assertEqual(done.returncode, 2)
        self.assertIn("APP", done.stderr)

    def test_a_file_name_is_not_a_module(self):
        done = _python("-m", "wasmkeep", "build", "app.py")
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("APP is a module name, not a file", done.stderr)
        self.assertNotIn("Traceback", done.stderr)

    def test_a_missing_module_names_where_it_looked(self):
        done = _python("-m", "wasmkeep", "build", "no_such_app", "-p", "test")
        self.assertNotEqual(done.returncode, 0)
        # The current directory is searched as well as the one named.
        self.assertRegex(done.stderr, r"no module no_such_app in \S+, \S+test\n")
        self.assertNotIn("Traceback", done.stderr)

    def test_a_host_function_says_where_it_runs(self):
        done = _python("-c", "import wasmkeep; wasmkeep.storage.exec('SELECT 1')")
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("RuntimeError: wasmkeep: the host's functions exist only inside a component", done.stderr)

    def test_exports_say_where_they_run(self):
        done = _python("-c", "import wasmkeep; wasmkeep.Actor.exports()")
        self.assertNotEqual(done.returncode, 0)
        self.assertIn("only inside a component", done.stderr)


if __name__ == "__main__":
    unittest.main()
