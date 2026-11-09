"""Read a soak's samples.csv (crates/wasmkeep-cli/tests/soak.rs) and report,
for each kill, how long the killed node's actors took to be writable again,
how late its alarms fired and how long the node took to restart.

Usage: python3 -I bench/soak_samples.py <soak dir>/samples.csv

"Writable again" is, per actor probed after a kill, the time from the kill
to the end of its first acknowledged probe write. Only Python's standard
library is used.
"""

import argparse
import csv
import sys


def percentile(sorted_values, p):
    """The soak's own rule: the value at the rank rounded half away from
    zero, as Rust's `f64::round` does (Python's `round` rounds half to
    even, which would pick another sample)."""
    if not sorted_values:
        return 0
    return sorted_values[int((len(sorted_values) - 1) * p + 0.5)]


def per_kill(rows):
    """One dict per kill, in order."""
    kills = []
    current = None
    writable = {}
    for row in rows:
        kind = row["kind"]
        at = int(row["at_ms"])
        if kind == "kill":
            current = {"at_ms": at, "node": row["node"], "writable_ms": [], "alarm_late_ms": [], "ready_ms": None}
            kills.append(current)
            writable = {}
        elif current is None:
            continue
        elif kind == "probe" and row["value"]:
            actor = row["actor"]
            if actor not in writable:
                writable[actor] = at + int(row["took_ms"]) - current["at_ms"]
                current["writable_ms"].append(writable[actor])
        elif kind == "alarm_fired":
            current["alarm_late_ms"].append(int(row["took_ms"]))
        elif kind == "ready":
            current["ready_ms"] = int(row["took_ms"])
    return kills


def report(rows):
    lines = ["kill,node,actors,writable_p50_ms,writable_p99_ms,writable_max_ms,alarms,alarm_late_max_ms,ready_ms"]
    every = []
    for n, kill in enumerate(per_kill(rows), start=1):
        w = sorted(kill["writable_ms"])
        every.extend(w)
        late = kill["alarm_late_ms"]
        lines.append(
            ",".join(
                str(v)
                for v in (
                    n,
                    kill["node"],
                    len(w),
                    percentile(w, 0.5),
                    percentile(w, 0.99),
                    w[-1] if w else 0,
                    len(late),
                    max(late) if late else 0,
                    "" if kill["ready_ms"] is None else kill["ready_ms"],
                )
            )
        )
    every.sort()
    lines.append(
        "all,,{},{},{},{},,,".format(
            len(every), percentile(every, 0.5), percentile(every, 0.99), every[-1] if every else 0
        )
    )
    return lines


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=argparse.FileType("r", encoding="utf-8"))
    args = parser.parse_args()
    with args.input as source:
        try:
            lines = report(list(csv.DictReader(source)))
        except (KeyError, ValueError) as error:
            parser.error(str(error))
    sys.stdout.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
