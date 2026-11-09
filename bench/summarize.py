"""Summarize repeated comparison CSV rows into columns bench,runs,median,min,max,unit.

Usage: python3 -I bench/summarize.py bench/results/<run>-ssd.csv > summary.csv
The input must use one commit, host and storage label, with one row per
metric and run. Units are preserved, never inferred from the metric name.
"""

import argparse
import csv
import statistics
import sys


def summarize(source):
    rows = list(csv.DictReader(source))
    if not rows:
        raise ValueError("no comparison rows")
    groups = {}
    seen = set()
    provenance = {(row["commit"], row["host"], row["storage"]) for row in rows}
    if len(provenance) != 1:
        raise ValueError("mixed commits, hosts or storage labels")
    for row in rows:
        key = (row["bench"], row["run"])
        if key in seen:
            raise ValueError("duplicate metric/run")
        seen.add(key)
        groups.setdefault(row["bench"], []).append(row)
    result = []
    for bench, group in sorted(groups.items()):
        units = {row["unit"] for row in group}
        if len(units) != 1:
            raise ValueError("mixed units for one metric")
        values = [int(row["value"]) for row in group]
        result.append((bench, len(values), statistics.median(values), min(values), max(values), units.pop()))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=argparse.FileType("r", encoding="utf-8"))
    args = parser.parse_args()
    try:
        with args.input as source:
            rows = summarize(source)
    except (KeyError, ValueError) as error:
        parser.error(str(error))
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(("bench", "runs", "median", "min", "max", "unit"))
    writer.writerows(rows)


if __name__ == "__main__":
    main()
