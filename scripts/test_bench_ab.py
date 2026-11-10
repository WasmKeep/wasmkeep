"""Tests for scripts/bench-ab.py (run by scripts/check.sh)."""

import importlib.util
import json
import pathlib
import tempfile
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("bench_ab", ROOT / "scripts" / "bench-ab.py")
bench_ab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench_ab)
REF = "hello/request/raw-hyper"


def write(root, directory, ns, full_id=None):
    new = root / directory / "new"
    new.mkdir(parents=True)
    (new / "estimates.json").write_text(json.dumps({"median": {"point_estimate": ns}}))
    if full_id:
        (new / "benchmark.json").write_text(json.dumps({"full_id": full_id}))


class Medians(unittest.TestCase):
    def test_reads_each_benchmarks_last_run(self):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            write(root, "actor/warm", 1500.0)
            write(root, "instantiate/store+instance", 900.0)
            base = root / "actor/warm/base"
            base.mkdir()
            (base / "estimates.json").write_text(json.dumps({"median": {"point_estimate": 1.0}}))
            self.assertEqual(
                bench_ab.medians(root),
                {"actor/warm": 1500.0, "instantiate/store+instance": 900.0},
            )

    def test_uses_criterions_full_id(self):
        with tempfile.TemporaryDirectory() as d:
            root = pathlib.Path(d)
            write(root, "hello/request_raw-hyper", 9.0, full_id=REF)
            self.assertEqual(bench_ab.medians(root), {REF: 9.0})
            self.assertIn(REF, bench_ab.REFERENCE)


class Ratios(unittest.TestCase):
    def test_pairs_each_rounds_sides(self):
        # Drift: everything doubles in round 2 on both sides; the ratio holds.
        rounds = [({"a": 100}, {"a": 110}), ({"a": 200}, {"a": 220})]
        per_bench, partial = bench_ab.ratios(rounds)
        self.assertEqual(per_bench, {"a": [1.1, 1.1]})
        self.assertEqual(partial, [])

    def test_benchmarks_missing_anywhere_are_listed_not_compared(self):
        rounds = [({"a": 1, "old": 1}, {"a": 1, "new": 1}), ({"a": 1, "b": 1}, {"a": 1})]
        per_bench, partial = bench_ab.ratios(rounds)
        self.assertEqual(list(per_bench), ["a"])
        self.assertEqual(partial, ["b", "new", "old"])

    def test_measured_in_some_rounds_only(self):
        rounds = [({"a": 1, "b": 1}, {"a": 1, "b": 1}), ({"a": 1}, {"a": 1})]
        per_bench, partial = bench_ab.ratios(rounds)
        self.assertEqual(list(per_bench), ["a"])
        self.assertEqual(partial, ["b"])


class Judge(unittest.TestCase):
    def test_within_threshold(self):
        rows, outcome = bench_ab.judge({"a": [1.19, 1.0, 1.25]}, 0.20)
        self.assertEqual((rows[0][2], outcome), ("ok", "ok"))

    def test_past_threshold(self):
        rows, outcome = bench_ab.judge({"a": [1.3, 1.25, 1.1], "b": [0.9] * 3}, 0.20)
        self.assertEqual([r[2] for r in rows], ["slower", "ok"])
        self.assertEqual(outcome, "regression")

    def test_reference_never_gates(self):
        rows, outcome = bench_ab.judge({REF: [2.0] * 3}, 0.20)
        self.assertEqual((rows[0][2], outcome), ("reference", "ok"))

    def test_a_noisy_reference_turns_a_regression_into_a_report(self):
        _, outcome = bench_ab.judge({"a": [1.5] * 3, REF: [1.2] * 3}, 0.20)
        self.assertEqual(outcome, "noisy")
        _, outcome = bench_ab.judge({"a": [1.5] * 3, REF: [1.05] * 3}, 0.20)
        self.assertEqual(outcome, "regression")


class Report(unittest.TestCase):
    def test_lists_rows_partial_and_noise(self):
        text = bench_ab.report([("a", 1.25, "slower")], "noisy", 0.20, "abc", ["b"])
        self.assertIn("| `a` | +25.0% | slower |", text)
        self.assertIn("`b`", text)
        self.assertIn("too noisy", text)


class Command(unittest.TestCase):
    def test_target_dir_is_a_flag_not_a_variable(self):
        cmd = bench_ab.bench_cmd(pathlib.Path("/t"), no_run=False)
        self.assertEqual(cmd[:5], ["cargo", "bench", "--locked", "--target-dir", "/t"])
        self.assertEqual(cmd[-2:], ["--", "--noplot"])
        self.assertIn("--no-run", bench_ab.bench_cmd(pathlib.Path("/t"), no_run=True))


if __name__ == "__main__":
    unittest.main()
