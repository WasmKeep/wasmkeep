"""Benchmark regression check: the same Criterion benchmarks for two commits,
on the same machine, interleaved; fails when one gets more than 20% slower.

    python3 -I scripts/bench-ab.py BASE [--rounds 3] [--threshold 0.20]

BASE is any git revision (CI passes the pull request's base commit). The
working tree is the other side ("head"). BASE is checked out as a detached
worktree in target/bench-ab/base and built into target/bench-ab/base-target
(cargo's --target-dir flag: a CARGO_TARGET_DIR variable would leak into the
nested cargo the benches use to build guest examples, which read their output
from each example's own target/). Criterion writes each side's results under
target/bench-ab/criterion-<side> (CRITERION_HOME), so target/criterion and
bench/results/ are left alone; numbers in docs come from bench/run.sh.

Each round runs both sides back to back, alternating which goes first, and
records head/base per benchmark; a benchmark's change is the median of its
per-round ratios, so drift across the job does not count. Three rounds at a
20% threshold catch gross regressions, not small ones.

`hello/request/raw-hyper` is plain hyper with no WasmKeep code: it never
gates, but if it moved by more than 15% the machine was too noisy and the run
only reports. A base that cannot be built or measured (an old tree, a
benchmark it does not have) is reported, not failed. Exit status: 1 on a
regression or a failure of the head side, else 0. The Markdown report is
printed and appended to $GITHUB_STEP_SUMMARY when set.
"""

import argparse
import json
import os
import pathlib
import shutil
import statistics
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
WORK = ROOT / "target" / "bench-ab"

# Fast, self-contained suites covering the request path: instantiation, HTTP
# through the server, actors warm, created and woken.
BENCHES = [
    ("wasmkeep-runtime", "instantiate"),
    ("wasmkeep-server", "http"),
    ("wasmkeep-server", "actors"),
]
REFERENCE = {"hello/request/raw-hyper"}
NOISE_LIMIT = 0.15


class SideFailed(Exception):
    def __init__(self, side, what):
        super().__init__(f"{side} side failed while {what}")
        self.side = side


def medians(criterion_dir):
    """Benchmark id -> median point estimate (ns) of the last run. The id is
    Criterion's full_id ("hello/request/raw-hyper"); its directory name
    replaces the slashes inside a function id ("hello/request_raw-hyper")."""
    out = {}
    for est in sorted(criterion_dir.glob("**/new/estimates.json")):
        meta = est.with_name("benchmark.json")
        if meta.exists():
            bench = json.loads(meta.read_text())["full_id"]
        else:
            bench = est.parent.parent.relative_to(criterion_dir).as_posix()
        out[bench] = json.loads(est.read_text())["median"]["point_estimate"]
    return out


def ratios(rounds):
    """Rounds of (base, head) {bench: ns} pairs -> {bench: [head/base per
    round]} for benchmarks both sides measured in that round; and the
    benchmarks some round measured on one side only."""
    out, partial = {}, set()
    for base, head in rounds:
        partial |= set(base) ^ set(head)
        for bench in set(base) & set(head):
            out.setdefault(bench, []).append(head[bench] / base[bench])
    complete = {b: r for b, r in out.items() if len(r) == len(rounds)}
    partial |= set(out) - set(complete)
    return complete, sorted(partial)


def judge(per_bench, threshold):
    """{bench: [ratios]} -> rows (bench, median ratio, verdict) and the
    outcome: "regression", "noisy" or "ok"."""
    rows = []
    for bench in sorted(per_bench):
        ratio = statistics.median(per_bench[bench])
        if bench in REFERENCE:
            verdict = "reference"
        elif ratio > 1 + threshold:
            verdict = "slower"
        else:
            verdict = "ok"
        rows.append((bench, ratio, verdict))
    noisy = any(abs(r - 1) > NOISE_LIMIT for b, r, _ in rows if b in REFERENCE)
    slower = any(v == "slower" for _, _, v in rows)
    return rows, "noisy" if noisy and slower else "regression" if slower else "ok"


def report(rows, outcome, threshold, base_rev, partial):
    lines = [
        f"### Benchmarks: this change against {base_rev}",
        "",
        f"Median of per-round ratios, same runner; fails past +{threshold:.0%}.",
        "",
        "| Benchmark | Change | Verdict |",
        "| --- | ---: | --- |",
    ]
    for bench, ratio, verdict in rows:
        lines.append(f"| `{bench}` | {ratio - 1:+.1%} | {verdict} |")
    if partial:
        lines += ["", "Not compared (missing on one side or in a round): "
                  + ", ".join(f"`{b}`" for b in partial)]
    if outcome == "noisy":
        lines += ["", f"The reference moved by more than {NOISE_LIMIT:.0%}: the runner was too "
                  "noisy to judge, so this run only reports."]
    return "\n".join(lines) + "\n"


def run(cmd, cwd, env, side, what):
    print("+", " ".join(cmd), f"[{side}]", flush=True)
    if subprocess.run(cmd, cwd=cwd, env=env).returncode != 0:
        raise SideFailed(side, what)


def bench_cmd(target_dir, no_run):
    cmd = ["cargo", "bench", "--locked", "--target-dir", str(target_dir)]
    for package, bench in BENCHES:
        cmd += ["-p", package, "--bench", bench]
    return cmd + (["--no-run"] if no_run else ["--", "--noplot"])


def git(*args, check=True):
    return subprocess.run(["git", *args], cwd=ROOT, check=check, capture_output=True, text=True)


def checkout(rev, tree):
    """A fresh detached worktree at `tree`, whatever an earlier run left."""
    git("worktree", "remove", "--force", str(tree), check=False)
    shutil.rmtree(tree, ignore_errors=True)
    git("worktree", "prune")
    WORK.mkdir(parents=True, exist_ok=True)
    git("worktree", "add", "--detach", str(tree), rev)


def summarize(text):
    print(text)
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a") as f:
            f.write(text)


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("base")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--threshold", type=float, default=0.20)
    args = parser.parse_args()

    base_rev = git("rev-parse", "--verify", f"{args.base}^{{commit}}").stdout.strip()
    tree = WORK / "base"
    env = {k: v for k, v in os.environ.items() if k not in ("CARGO_TARGET_DIR", "CRITERION_HOME")}
    sides = {
        "base": (tree, WORK / "base-target"),
        "head": (ROOT, ROOT / "target"),
    }
    homes = {name: WORK / f"criterion-{name}" for name in sides}
    rounds = []
    checkout(base_rev, tree)
    try:
        for name in ("head", "base"):
            cwd, target = sides[name]
            run(bench_cmd(target, no_run=True), cwd, env, name, "building")
        for r in range(args.rounds):
            measured = {}
            for name in (["base", "head"] if r % 2 == 0 else ["head", "base"]):
                cwd, target = sides[name]
                shutil.rmtree(homes[name], ignore_errors=True)
                run(bench_cmd(target, no_run=False), cwd,
                    dict(env, CRITERION_HOME=str(homes[name])), name, f"measuring round {r + 1}")
                measured[name] = medians(homes[name])
            rounds.append((measured["base"], measured["head"]))
    except SideFailed as e:
        summarize(f"### Benchmarks: this change against {base_rev[:12]}\n\n{e}.\n")
        if e.side == "base":
            summarize("The base could not be measured, so nothing was compared.\n")
            return 0
        return 1
    finally:
        git("worktree", "remove", "--force", str(tree), check=False)
        git("worktree", "prune", check=False)

    per_bench, partial = ratios(rounds)
    if not per_bench:
        summarize("No benchmark was measured on both sides in every round.\n")
        return 0
    rows, outcome = judge(per_bench, args.threshold)
    summarize(report(rows, outcome, args.threshold, base_rev[:12], partial))
    return 1 if outcome == "regression" else 0


if __name__ == "__main__":
    sys.exit(main())
