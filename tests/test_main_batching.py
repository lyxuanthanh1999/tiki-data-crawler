import json
import tempfile
import unittest
from pathlib import Path

from main import (
    batch_output_path,
    chunk_ids,
    seed_parts_from_existing_output,
    summarize_batch,
)


class MainBatchingTests(unittest.IsolatedAsyncioTestCase):
    def test_chunk_ids_keeps_order_and_batch_size(self):
        batches = chunk_ids(list(range(1, 2502)), 1000)

        self.assertEqual(len(batches), 3)
        self.assertEqual(len(batches[0]), 1000)
        self.assertEqual(len(batches[1]), 1000)
        self.assertEqual(len(batches[2]), 501)
        self.assertEqual(batches[0][0], 1)
        self.assertEqual(batches[2][-1], 2501)

    def test_batch_output_path_is_stable(self):
        self.assertEqual(
            batch_output_path(Path("data/output/concurrency/parts"), 12),
            Path("data/output/concurrency/parts/products_part_0012.json"),
        )

    def test_batch_range_slice_uses_original_batch_numbering(self):
        batches = chunk_ids(list(range(1, 2501)), 1000)
        selected = [
            (index, batches[index - 1])
            for index in range(2, 3)
        ]

        self.assertEqual(selected[0][0], 2)
        self.assertEqual(selected[0][1][0], 1001)
        self.assertEqual(
            batch_output_path(Path("parts"), selected[0][0]),
            Path("parts/products_part_0002.json"),
        )

    async def test_seed_parts_from_existing_output(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            seed_output = root / "products_output.json"
            parts_dir = root / "parts"
            seed_products = [
                {
                    "id": 1,
                    "name": "P1",
                    "url_key": "p1",
                    "price": 100,
                    "description": "D1",
                    "images_url": [],
                },
                {
                    "id": 4,
                    "name": "P4",
                    "url_key": "p4",
                    "price": 400,
                    "description": "D4",
                    "images_url": [],
                },
            ]
            seed_output.write_text(
                json.dumps(seed_products, ensure_ascii=False),
                encoding="utf-8",
            )
            seed_output.with_suffix(".failed_permanent.json").write_text(
                json.dumps({"5": {"reason": "http_404", "attempts": 1}}),
                encoding="utf-8",
            )

            summary = await seed_parts_from_existing_output(
                [1, 2, 3, 4, 5],
                3,
                parts_dir,
                seed_output,
            )

            self.assertEqual(summary["products_seeded"], 2)
            self.assertEqual(summary["failed_seeded"], 1)
            self.assertEqual(summary["batches_touched"], 2)
            self.assertEqual(
                [item["id"] for item in json.loads((parts_dir / "products_part_0001.json").read_text())],
                [1],
            )
            self.assertEqual(
                [item["id"] for item in json.loads((parts_dir / "products_part_0002.json").read_text())],
                [4],
            )
            self.assertIn(
                "5",
                json.loads(
                    (parts_dir / "products_part_0002.failed_permanent.json").read_text()
                ),
            )

    async def test_summarize_batch_counts_statuses(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "products_part_0001.json"
            await seed_parts_from_existing_output(
                [1, 2, 3],
                1000,
                Path(temp_dir),
                output,
            )
            output.write_text(
                json.dumps(
                    [
                        {
                            "id": 1,
                            "name": "P1",
                            "url_key": "p1",
                            "price": 100,
                            "description": "D1",
                            "images_url": [],
                        }
                    ]
                ),
                encoding="utf-8",
            )
            output.with_suffix(".failed_permanent.json").write_text(
                json.dumps({"2": {"reason": "http_404", "attempts": 1}}),
                encoding="utf-8",
            )

            summary = summarize_batch([1, 2, 3], output)

            self.assertEqual(summary["success"], 1)
            self.assertEqual(summary["permanent_failed"], 1)
            self.assertEqual(summary["done"], 2)
            self.assertEqual(summary["due"], 1)
            self.assertEqual(summary["retry_pending"], 0)

    def test_wait_for_cooldown_returns_true_when_no_cooldown(self):
        from unittest.mock import MagicMock
        from main import wait_for_cooldown

        mock_store = MagicMock()
        mock_store.global_wait_remaining.return_value = 0

        result = wait_for_cooldown(mock_store, batch_index=1, auto_wait_interval=0.01, max_retries=2)
        self.assertTrue(result)

    def test_wait_for_cooldown_returns_false_when_max_retries_exceeded(self):
        from unittest.mock import MagicMock
        from main import wait_for_cooldown

        mock_store = MagicMock()
        mock_store.global_wait_remaining.return_value = 100

        result = wait_for_cooldown(mock_store, batch_index=1, auto_wait_interval=0.01, max_retries=2)
        self.assertFalse(result)


if __name__ == "__main__":
    unittest.main()

