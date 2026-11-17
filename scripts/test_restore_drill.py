"""Safety checks for the drill's source/scratch boundary and ack input."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location(
    "restore_drill", Path(__file__).parent / "drills/backup_restore.py")
drill = importlib.util.module_from_spec(spec)
spec.loader.exec_module(drill)


class DrillSafety(unittest.TestCase):
    def test_overlapping_roots_are_refused(self):
        for source, scratch in [("s3://b", "s3://b/drill"),
                                ("s3://b/live", "s3://b/live"),
                                ("s3://b/live", "s3://b/live/drill"),
                                ("s3://b/live/child", "s3://b/live")]:
            with self.subTest(source=source, scratch=scratch), self.assertRaises(ValueError):
                drill.disjoint(source, scratch)
        drill.disjoint("s3://b/live", "s3://b/live-drill")
        drill.disjoint("s3://b", "s3://other/drill")

    def test_acknowledgements_are_nonempty_durable_fixture_routes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "acks.json"
            for data in [{}, {"/actor/t/a/durable": 0},
                         {"/actor/t/a/durable": True},
                         {"/actor/t/a/fetch?url=https://example.com": 1}]:
                path.write_text(json.dumps(data))
                with self.assertRaises(ValueError):
                    drill.floors(path)
            good = {"/actor/t/s123-1/durable": 42}
            path.write_text(json.dumps(good))
            self.assertEqual(drill.floors(path), good)


if __name__ == "__main__":
    unittest.main()
