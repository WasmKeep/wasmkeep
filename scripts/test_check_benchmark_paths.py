"""Exercise publication checks against Git blobs and the complete PR history."""

import pathlib
import subprocess
import sys
import tempfile
import unittest


CHECKER = pathlib.Path(__file__).with_name("check-benchmark-paths.py")


class PublicationPaths(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="benchmark-path-test-")
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name)
        self.git("init", "--quiet")

    def git(self, *args):
        return subprocess.check_output(
            ["git", *args], cwd=self.root, text=True, stderr=subprocess.PIPE
        ).strip()

    def stage(self, name, text):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        self.git("add", "--", name)
        return path

    def commit(self, message):
        self.git(
            "-c", "user.name=Publication Test", "-c", "user.email=test@example.invalid",
            "-c", "commit.gpgsign=false", "commit", "--quiet", "--allow-empty", "-m", message,
        )
        return self.git("rev-parse", "HEAD")

    def check(self, *args, cwd=None):
        return subprocess.run(
            [sys.executable, "-I", str(CHECKER), *args],
            cwd=cwd or self.root,
            text=True,
            capture_output=True,
        )

    def test_relative_paths_and_placeholders_are_publishable(self):
        self.stage(
            "bench/results/example.log",
            "[repo]/crates/runtime\n[scratch]/data/actor.db\n"
            "target/bench-ssd-tmp\n${TMPDIR}/actor.db\n",
        )
        self.stage("src/example.txt", "/home/example/private\n")
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_private_paths_are_reported_without_echoing_them(self):
        for private in (
            "/home/example/Projects/private", "/Users/example/Documents/private",
            "/Volumes/Personal/private", "/root/private", "/tmp/benchmark-id/data",
            "/var/folders/secret/scratch", "/dev/shm/private", "/run/user/1234/private",
            "/mnt/private/data", "/media/private/data", "/opt/private/data",
            "/workspace/private/data", "/unknown-root/private", "/private/tmp/private",
            r"C:\Users\example\private", r"C:\\Users\\example\\private",
            r"D:\Projects\private", "D:/Projects/private", "~/Projects/private",
            "[raw log](~/Projects/private/run.log)",
            r"\\private-server\share\report", "file:///mnt/private/data",
            "file:/opt/private/data", "file://localhost/workspace/private",
            "path:/mnt/private/data", "error:/home/example/private.rs:12: error",
            r"\\\\private-server\\share\\report",
            r"\\newserver\share", r"\\testserver\share", r"\\rootserver\share",
            r"C:\temp\report", r"D:\new\logs",
        ):
            with self.subTest(private=private):
                self.stage("docs/notes/example.md", private + "\n")
                result = self.check()
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn("docs/notes/example.md:1:", result.stderr)
                self.assertNotIn(private, result.stderr)

    def test_encoded_private_paths_are_rejected(self):
        for private in (
            r"\/home\/example\/private", r"\u002fhome\u002fexample\u002fprivate",
            r"\u002Fopt\u002Fprivate", "%2Fmnt%2Fprivate", "%252Fworkspace%252Fprivate",
            r"D:\u005cProjects\u005cprivate", "D%3A%5CProjects%5Cprivate",
        ):
            with self.subTest(private=private):
                self.stage("bench/results/example.json", private)
                result = self.check()
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertNotIn(private, result.stderr)

    def test_bare_directory_mentions_and_system_paths_are_publishable(self):
        self.stage(
            "docs/example.md",
            "The ~/ and /tmp/ conventions; TMPDIR=/tmp.\n"
            "`/proc/1234/status`, `/usr/bin/python3`, `/bin/sh`, `/etc/wasmkeep/env`.\n"
            "echo test >/dev/null\n",
        )
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_safe_prefix_cannot_hide_a_joined_private_path(self):
        for value in (
            "PATH=/usr/local/bin:/home/alice/.cargo/bin",
            "/etc/x,/home/alice/y", "/bin/sh;/mnt/private/data",
            "/dev/null:/workspace/private/data",
        ):
            with self.subTest(value=value):
                self.stage("docs/example.md", value)
                self.assertEqual(self.check().returncode, 1)

    def test_shell_redirection_does_not_hide_an_absolute_path(self):
        for value in (
            "command >/home/alice/private.log", "command 2>/mnt/private/error.log",
            "command >>/workspace/private/output.log", "<unknown>/home/alice/private",
        ):
            with self.subTest(value=value):
                self.stage("docs/example.md", value)
                self.assertEqual(self.check().returncode, 1)

    def test_escaped_whitespace_separates_logged_paths(self):
        for separator in (r"\n", r"\t", r"\r"):
            with self.subTest(separator=separator):
                self.stage("bench/results/example.json", '"/usr/bin/tool' + separator + '/home/alice/private"')
                self.assertEqual(self.check().returncode, 1)

    def test_emphasis_and_punctuation_do_not_hide_private_paths(self):
        for value in (
            "**/home/alice/private**", "*/home/alice/private*",
            "_/home/alice/private_", "__/mnt/private__", "100%/home/alice/private",
            "**%2Fhome%2Falice%2Fprivate**", "(**/workspace/private)",
            "location)/home/alice/private", ")/mnt/private",
        ):
            with self.subTest(value=value):
                self.stage("docs/example.md", value)
                result = self.check()
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertNotIn(value, result.stderr)

    def test_named_home_directories_are_private(self):
        for value in (
            "~alice/secret", "~build-user/logs", "~user_name/private",
            "[private](~alice/secret)", "~alice/", "~alice%2Fsecret",
            "~josé/secret",
        ):
            with self.subTest(value=value):
                self.stage("bench/results/example.log", value)
                self.assertEqual(self.check().returncode, 1)

    def test_adjacent_escaped_notation_is_not_a_unc_path(self):
        for value in (
            r'"\\s\\d+"', r'{"regexp":"\\s\\d+"}',
            r'pattern = "\\s\\d+"', r'pattern = "\\w\\s+\\d{2}"',
            r"printf '\\n\\tindent'", r'printf "\\r\\n\\tindent"',
        ):
            with self.subTest(value=value):
                self.stage("docs/example.md", value)
                result = self.check()
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_escaped_notation_cannot_hide_private_paths(self):
        for value in (
            r'pattern = "\\s\\d+/home/alice/private"',
            r"printf '\\n\\tindent/home/alice/private'",
            r"printf '\\n\\tindent' /home/alice/private",
            r"printf '\\n\\tindent' \\private-server\share",
            r"printf '\\n\\tindent\\private-server\share'",
            r'UNC = "\\s\disk"', r'UNC = "\\newserver\share"',
            r'UNC = "\\testserver\share"', r'UNC = "\\rootserver\share"',
            r'pattern = "D:\\s\\d+"', r'printf "C:\\n\\tindent"',
            r'UNC = "\\private-server\share\\s\\d+"',
        ):
            with self.subTest(value=value):
                self.stage("docs/example.md", value)
                self.assertEqual(self.check().returncode, 1)

    def test_valid_ipv6_cidrs_are_publishable_in_any_scanned_file(self):
        for name in ("docs/new-guide.md", "bench/results/example.log"):
            with self.subTest(name=name):
                self.stage(name, "2001:db8::/32 ::/0 ::1/128 ::ffff:192.0.2.1/128")
                result = self.check()
                self.assertEqual(result.returncode, 0, result.stderr)

    def test_notation_exceptions_require_a_complete_token(self):
        for raw in (
            r"printf '\\n\\tprivate\customer_a'",
            r"printf '\\n\\tprivate.log'",
            r'UNC = "\\s\\d-private-company"',
            r'UNC = "\\s\\d.private"',
            r'UNC = "\\private-server\\s\\d+"',
            r"printf '\\private-server\\n\\tindent'",
            r"printf '\\n\\tprivate-private'",
            r"printf '\\n\\tcustomer_a'",
        ):
            for value in (raw, raw.replace("\\", "%5C"), raw.replace("\\", r"\u005c")):
                with self.subTest(value=value):
                    self.stage("docs/example.md", value)
                    result = self.check()
                    self.assertEqual(result.returncode, 1, result.stderr)
                    self.assertNotIn(value, result.stderr)

    def test_unc_filename_punctuation_is_not_a_notation_boundary(self):
        for suffix in ("(private)", "[private]", ",private", ";private", " private",
                       "'private", chr(96) + "private"):
            for raw in (
                'UNC = "' + r"\\s\\d" + suffix + '"',
                "printf '" + r"\\n\\tprivate" + suffix + "'",
            ):
                for value in (raw, raw.replace("\\", "%5C"), raw.replace("\\", r"\u005c")):
                    with self.subTest(value=value):
                        self.stage("docs/example.md", value)
                        result = self.check()
                        self.assertEqual(result.returncode, 1, result.stderr)

    def test_ipv6_cidr_masks_are_bounded_and_validated(self):
        for value in (
            "2001:db8::/129", "2001:db8:::/32", "2001:db8::/32suffix",
            "2001:db8::/32/home/alice/private", "2001:db8::/32,/home/alice/private",
            "2001:db8::/32:/mnt/private", "2001:db8::/32 /workspace/private",
        ):
            with self.subTest(value=value):
                self.stage("docs/new-guide.md", value)
                self.assertEqual(self.check().returncode, 1)

    def test_parent_components_cannot_escape_a_permitted_prefix(self):
        for value in (
            "/usr/../../home/alice/private", "/etc/../mnt/private",
            "/proc/../home/alice", "/actor/../../home/alice",
            "/_wasmkeep/../mnt/private", "[site](/docs/../../home/alice)",
            "/usr/..", "/usr/%2e%2e/home/alice",
        ):
            with self.subTest(value=value):
                self.stage("docs/example.md", value)
                self.assertEqual(self.check().returncode, 1)

    def test_escaped_regular_expressions_are_not_unc_paths(self):
        self.stage("docs/example.md", r'pattern = "\\d+"; newline = "\\n"')
        self.assertEqual(self.check().returncode, 0)
        self.stage("docs/example.md", r'UNC = "\\private-server\private-share"')
        self.assertEqual(self.check().returncode, 1)

    def test_public_urls_with_directory_names_are_publishable(self):
        self.stage(
            "docs/example.md",
            "[home](https://example.invalid/home/example)\n"
            "https://example.invalid/root/guide\n"
            "https://example.invalid/%2Fhome%2Fexample\n",
        )
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_relative_globs_templates_and_url_templates_are_publishable(self):
        self.stage(
            "docs/example.md",
            "crates/*/tests/\nbuild_/logs\ncache/<app>/<version>/component.cwasm\n"
            "s3://BUCKET[/PREFIX]\n<stage>/docs\n",
        )
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_reviewed_network_notation_does_not_hide_other_paths(self):
        example = "64:ff9b:1::/48, ::ffff:0:0/96, 2000::/3"
        self.stage("docs/notes.md", example)
        self.assertEqual(self.check().returncode, 0)
        self.stage("docs/notes.md", example + " error:/mnt/private/data")
        self.assertEqual(self.check().returncode, 1)

    def test_reviewed_benchmark_shell_syntax_is_exactly_scoped(self):
        example = (
            "find target/criterion -path '*/new/estimates.json'\n"
            "bench=${bench%/new/estimates.json}\n"
            'cd "$(dirname "$0")/.."'
        )
        self.stage("bench/run.sh", example)
        self.assertEqual(self.check().returncode, 0)
        for line in example.splitlines():
            self.stage("bench/run.sh", line + " /home/alice/private")
            self.assertEqual(self.check().returncode, 1)
        self.stage("bench/run.sh", example)
        self.stage("docs/new-guide.md", example)
        self.assertEqual(self.check().returncode, 1)

    def assert_each(self, lines, code):
        for text in lines:
            with self.subTest(text=text):
                self.stage("docs/example.md", text + "\n")
                result = self.check()
                self.assertEqual(result.returncode, code, result.stderr)

    def test_angle_bracket_placeholders_are_publishable(self):
        self.assert_each(
            [
                "`alarms/<tenant>/<at>/<class>/<name as hex>`",
                "`<at>/<class>.t/<hex>/<tag>`",
                "`<due ms, 15 digits>/<class>`",
                "a <b>bold</b> word and x < y > z",
            ],
            0,
        )

    def test_placeholders_and_tags_cannot_hide_a_home_directory(self):
        self.assert_each(
            [
                "`<at>/home/alice/x`",
                "[INFO]/home/alice/x",
                "{x}/Users/alice/x",
                "${HOME}/home/alice/x",
            ],
            1,
        )

    def test_a_url_does_not_hide_a_path_joined_to_it(self):
        self.assert_each(
            [
                "https://example.com/a,/home/alice/x",
                "https://example.com/a;/home/alice/x",
                "https://example.com/a|/home/alice/x",
                "(https://example.com/a)/home/alice/x",
                "https://example.com/a?file=/home/alice/x",
                "https://example.com/a#/home/alice/x",
            ],
            1,
        )
        self.assert_each(["https://example.com/a,b", "https://example.com/x?limit=20"], 0)

    def test_extra_slashes_options_and_colour_codes_do_not_hide_paths(self):
        self.assert_each(
            [
                "///home/alice/x",
                "////mnt/private",
                "cc -I/home/alice/include",
                "ld -L/opt/private/lib",
                "\x1b[31m/home/alice/x",
                "\u001b[0m/home/alice/x",
            ],
            1,
        )
        self.assert_each(["// a comment", "/// a doc comment"], 0)

    def test_system_roots_and_unc_prefixes_do_not_hide_home_directories(self):
        self.assert_each(
            [
                "/usr/home/alice/x",
                "/proc/1234/root/home/x",
                "/proc/self/cwd/x",
                "\\\\?\\UNC\\server\\share",
                "\\\\.\\UNC\\server\\share",
            ],
            1,
        )

    def test_url_does_not_hide_an_independent_private_path_on_same_line(self):
        self.stage("docs/example.md", "https://example.invalid/home/ /mnt/private/data\n")
        self.assertEqual(self.check().returncode, 1)

    def test_all_documentation_and_benchmark_artifacts_are_covered(self):
        for name in ("docs/benchmarks.md", "docs/notes.md", "docs/nested/report.md", "bench/output.log"):
            with self.subTest(name=name):
                self.stage(name, "/mnt/private/data\n")
                result = self.check()
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(name + ":1:", result.stderr)
                self.stage(name, "[scratch]/data\n")

    def test_test_directories_are_not_exempt_from_privacy_checks(self):
        self.stage("bench/compare/tests/preflight.rs", 'let fixture = "/tmp/private";\n')
        self.assertEqual(self.check().returncode, 1)
        self.stage("bench/compare/tests/preflight.rs", 'let fixture = "[scratch]/private";\n')
        self.stage("docs/tests/evidence.md", "/tmp/private\n")
        self.assertEqual(self.check().returncode, 1)

    def test_reviewed_deployment_paths_are_scoped_to_their_document(self):
        self.stage("docs/deploy/single-node.md", "--data-dir /var/lib/wasmkeep/data\n")
        self.assertEqual(self.check().returncode, 0)
        self.stage("docs/deploy/single-node.md", "/var/lib/wasmkeep/private-project\n")
        self.assertEqual(self.check().returncode, 1)
        self.stage("docs/deploy/single-node.md", "--data-dir /var/lib/wasmkeep/data\n")
        self.stage("docs/notes/example.md", "/var/lib/wasmkeep/data\n")
        self.assertEqual(self.check().returncode, 1)

    def test_reviewed_http_routes_do_not_exempt_a_whole_line_or_file(self):
        self.stage("docs/concepts.md", "GET /actor/chat/lobby/history?limit=20\n")
        self.assertEqual(self.check().returncode, 0)
        self.stage("docs/concepts.md", "GET /actor/chat/lobby/history?limit=20 /mnt/private\n")
        self.assertEqual(self.check().returncode, 1)
        self.stage("docs/concepts.md", "/actors/private/customer/data\n")
        self.assertEqual(self.check().returncode, 1)

    def test_public_routes_are_accepted_throughout_documentation(self):
        self.stage(
            "docs/new-guide.md",
            "GET /actor/counter/alice\nGET /_wasmkeep/ready\nGET /metrics\n",
        )
        self.assertEqual(self.check().returncode, 0)
        self.stage("docs/new-guide.md", "/metrics/private\n")
        self.assertEqual(self.check().returncode, 1)
        self.stage("docs/new-guide.md", "/metrics\n")
        self.stage("bench/results/output.log", "/actor/private-directory\n")
        self.assertEqual(self.check().returncode, 1)

    def test_front_door_namespace_example_does_not_hide_private_paths(self):
        self.stage("docs/deploy/front-door.md", "The first segment is `/actor`.")
        self.assertEqual(self.check().returncode, 0)
        self.stage("docs/deploy/front-door.md", "`/actor`,/home/alice/private")
        self.assertEqual(self.check().returncode, 1)
        self.stage("docs/deploy/front-door.md", "`/actor`")
        self.stage("bench/results/output.log", "/actor")
        self.assertEqual(self.check().returncode, 1)

    def test_markdown_site_links_do_not_exempt_personal_destinations(self):
        self.stage("docs/new-guide.md", "[Guide](/docs/concepts.html)\n")
        self.assertEqual(self.check().returncode, 0)
        for value in (
            "[private](/home/alice/private)", "[private](/mnt/private)",
            "[public](/docs/concepts.html),/home/alice/private",
        ):
            with self.subTest(value=value):
                self.stage("docs/new-guide.md", value)
                self.assertEqual(self.check().returncode, 1)

    def test_clean_working_copy_does_not_hide_private_staged_content(self):
        path = self.stage("bench/results/example.log", "/home/example/private\n")
        path.write_text("[repo]/private\n")
        result = self.check()
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("bench/results/example.log:1:", result.stderr)

    def test_revision_reads_that_tree_instead_of_the_index(self):
        self.stage("bench/results/example.log", "/home/example/private\n")
        unsafe_tree = self.git("write-tree")
        self.stage("bench/results/example.log", "[repo]/private\n")
        self.assertEqual(self.check().returncode, 0)
        result = self.check("--rev", unsafe_tree)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("bench/results/example.log:1:", result.stderr)

    def test_nested_working_directory_cannot_skip_staged_evidence(self):
        base = self.commit("base")
        self.stage("bench/results/example.log", "/home/example/private\n")
        result = self.check(cwd=self.root / "bench")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("bench/results/example.log:1:", result.stderr)
        self.commit("unsafe evidence")
        result = self.check("--range", base + "..HEAD", cwd=self.root / "bench")
        self.assertEqual(result.returncode, 1, result.stderr)

    def test_gitlinks_are_not_read_as_blobs(self):
        commit = self.commit("submodule target")
        self.git("update-index", "--add", "--cacheinfo", f"160000,{commit},bench/results/submodule")
        self.assertEqual(self.check().returncode, 0)
        self.assertEqual(self.check("--rev", self.git("write-tree")).returncode, 0)
        linked = self.commit("add gitlink")
        self.assertEqual(self.check("--range", commit + ".." + linked).returncode, 0)

    def test_range_catches_private_content_removed_before_head(self):
        base = self.commit("base")
        self.stage("docs/example.md", "/mnt/private/data\n")
        self.commit("unsafe intermediate version")
        self.stage("docs/example.md", "[scratch]/data\n")
        self.commit("redaction")
        self.assertEqual(self.check("--rev", "HEAD").returncode, 0)
        result = self.check("--range", base + "..HEAD")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("docs/example.md:1:", result.stderr)
        self.assertNotIn("/mnt/private/data", result.stderr)

    def test_range_does_not_rescan_untouched_files_inherited_from_old_base(self):
        self.stage("docs/old-guide.md", "/mnt/private/inherited\n")
        old_base = self.commit("old main")
        self.stage("docs/old-guide.md", "[scratch]/inherited\n")
        current_main = self.commit("redact on main")
        self.git("checkout", "--quiet", "--detach", old_base)
        self.stage("src/feature.rs", "// unrelated feature\n")
        feature = self.commit("feature based on old main")
        result = self.check("--range", current_main + ".." + feature)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Checked 0 documentation/benchmark blob versions", result.stdout)
        self.assertEqual(self.check("--rev", feature).returncode, 1)

    def test_range_still_checks_an_inherited_file_when_it_changes(self):
        self.stage("docs/example.md", "/mnt/private/inherited\n")
        base = self.commit("base")
        self.stage("docs/example.md", "/mnt/private/inherited\nNew public text.\n")
        self.commit("changed documentation")
        self.assertEqual(self.check("--range", base + "..HEAD").returncode, 1)

    def test_range_checks_content_created_only_in_a_merge(self):
        base = self.commit("base")
        self.stage("docs/feature.md", "Feature documentation.\n")
        feature = self.commit("feature")
        self.git("checkout", "--quiet", "--detach", base)
        self.stage("src/main.rs", "// main advances\n")
        main = self.commit("main")
        self.stage("docs/merge.md", "/mnt/private/merge-only\n")
        tree = self.git("write-tree")
        merged = self.git(
            "-c", "user.name=Publication Test", "-c", "user.email=test@example.invalid",
            "-c", "commit.gpgsign=false", "commit-tree", tree,
            "-p", main, "-p", feature, "-m", "merge result",
        )
        result = self.check("--range", base + ".." + merged)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("docs/merge.md:1:", result.stderr)

    def test_range_checks_root_commits_from_unrelated_history(self):
        base = self.commit("base")
        self.stage("docs/root.md", "/mnt/private/root-only\n")
        root = self.git(
            "-c", "user.name=Publication Test", "-c", "user.email=test@example.invalid",
            "-c", "commit.gpgsign=false", "commit-tree", self.git("write-tree"),
            "-m", "unrelated root",
        )
        result = self.check("--range", base + ".." + root)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("docs/root.md:1:", result.stderr)

    def test_range_checks_intermediate_side_branch_commits_after_merge(self):
        base = self.commit("base")
        self.stage("docs/feature.md", "/mnt/private/side-branch\n")
        self.commit("unsafe side branch version")
        self.stage("docs/feature.md", "[scratch]/redacted\n")
        feature = self.commit("redacted side branch")
        self.git("checkout", "--quiet", "--detach", base)
        self.stage("src/main.rs", "// main advances\n")
        main = self.commit("main")
        merged = self.git(
            "-c", "user.name=Publication Test", "-c", "user.email=test@example.invalid",
            "-c", "commit.gpgsign=false", "commit-tree", self.git("write-tree"),
            "-p", main, "-p", feature, "-m", "merge result",
        )
        self.assertEqual(self.check("--rev", merged).returncode, 0)
        result = self.check("--range", base + ".." + merged)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("docs/feature.md:1:", result.stderr)

    def test_range_checks_renamed_identical_blob_at_new_path(self):
        base = self.commit("base")
        self.stage("docs/deploy/single-node.md", "/var/lib/wasmkeep/data\n")
        self.commit("reviewed deployment example")
        self.stage("docs/notes/example.md", "/var/lib/wasmkeep/data\n")
        self.commit("same blob in evidence")
        result = self.check("--range", base + "..HEAD")
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("docs/notes/example.md:1:", result.stderr)

    def test_clean_range_and_empty_range_are_publishable(self):
        base = self.commit("base")
        self.stage("docs/example.md", "[repo]/project\n")
        self.commit("clean evidence")
        self.assertEqual(self.check("--range", base + "..HEAD").returncode, 0)
        self.assertEqual(self.check("--range", "HEAD..HEAD").returncode, 0)

    def test_invalid_range_and_unavailable_history_fail_closed(self):
        self.commit("base")
        for value in ("HEAD...HEAD", "HEAD", "missing-base..HEAD", "--all..HEAD"):
            with self.subTest(value=value):
                result = self.check("--range=" + value)
                self.assertEqual(result.returncode, 2, result.stderr)
                self.assertNotIn(str(self.root), result.stderr)


if __name__ == "__main__":
    unittest.main()
