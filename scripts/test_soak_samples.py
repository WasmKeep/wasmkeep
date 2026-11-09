"""Tests for bench/soak_samples.py (run by scripts/check.sh)."""

import csv
import importlib.util
import io
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("soak_samples", ROOT / "bench" / "soak_samples.py")
soak_samples = importlib.util.module_from_spec(spec)
spec.loader.exec_module(soak_samples)

SAMPLES = """at_ms,kind,actor,node,took_ms,value,ran_on
1000,write,1,a,40,7,a
2000,kill,,a,,,
2100,probe,1,b,30,,
2400,probe,1,b,50,,
15000,probe,1,c,100,8,c
15200,probe,1,c,100,9,c
16000,probe,2,b,500,3,b
17000,alarm_fired,3,b,2500,,
80000,ready,,a,4000,,
90000,kill,,b,,,
100000,probe,1,a,1000,10,a
"""


def rows():
    return list(csv.DictReader(io.StringIO(SAMPLES)))


class SoakSamplesTest(unittest.TestCase):
    def test_writable_again_is_the_first_acknowledged_probe_after_the_kill(self):
        kills = soak_samples.per_kill(rows())
        self.assertEqual(len(kills), 2)
        first = kills[0]
        self.assertEqual(first["node"], "a")
        # Actor 1: acknowledged at 15000 + 100 = 13100 ms after the kill;
        # its later acknowledged probe does not count. Actor 2: 14500 ms.
        self.assertEqual(sorted(first["writable_ms"]), [13100, 14500])
        self.assertEqual(first["alarm_late_ms"], [2500])
        self.assertEqual(first["ready_ms"], 4000)
        # Each kill starts afresh: actor 1 counts again after the second.
        self.assertEqual(kills[1]["writable_ms"], [11000])

    def test_the_report_has_a_line_per_kill_and_one_for_all(self):
        lines = soak_samples.report(rows())
        self.assertEqual(lines[0].split(",")[0], "kill")
        self.assertEqual(lines[1], "1,a,2,14500,14500,14500,1,2500,4000")
        self.assertEqual(lines[-1], "all,,3,13100,14500,14500,,,")


if __name__ == "__main__":
    unittest.main()
