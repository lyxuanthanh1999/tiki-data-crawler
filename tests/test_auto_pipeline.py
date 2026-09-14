import json
import tempfile
import unittest
from pathlib import Path

from tools.auto_pipeline import ids_hash, merge_run, read_ids


class AutoPipelineTests(unittest.TestCase):
    def test_failed_hash_is_order_independent(self):
        self.assertEqual(ids_hash(["3", "1", "2"]), ids_hash(["2", "3", "1"]))

    def test_read_ids_deduplicates_and_sorts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "ids.txt"
            path.write_text("3\n1\n2\n2\n", encoding="utf-8")
            self.assertEqual(read_ids(path), ["1", "2", "3"])

    def test_merge_run_only_reads_run_parts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "input.txt"
            source.write_text("1\n2\n", encoding="utf-8")
            parts = root / "phase_0001" / "parts"
            parts.mkdir(parents=True)
            (parts / "products_part_0001.json").write_text(
                json.dumps([{"id": 1, "name": "one"}]), encoding="utf-8"
            )
            failed, code = merge_run(root, source)
            self.assertEqual(code, 0)
            self.assertEqual(failed, ["2"])
            self.assertTrue((root / "merged" / "products_output.json").exists())


if __name__ == "__main__":
    unittest.main()
