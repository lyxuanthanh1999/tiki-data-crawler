import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fetch_tiki_products import ResultStore


class ResultStoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_product_is_durable_and_resumable(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "products.json"
            store = ResultStore(output)
            product = {
                "id": 123,
                "name": "Test",
                "url_key": "test-p123",
                "price": 100,
                "description": "Mo ta",
                "images_url": [],
            }

            self.assertTrue(await store.save_product(product))
            self.assertFalse(await store.save_product(product))

            resumed = ResultStore(output)
            self.assertEqual(resumed.successful_ids, {123})
            self.assertEqual(json.loads(output.read_text(encoding="utf-8")), [product])
            self.assertEqual(
                output.with_suffix(".jsonl").read_text(encoding="utf-8").count("\n"),
                1,
            )

    async def test_retry_schedule_and_global_cooldown_are_persisted(self):
        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "fetch_tiki_products.time.time", return_value=1000
        ):
            output = Path(temp_dir) / "products.json"
            store = ResultStore(output)

            self.assertEqual(
                await store.record_failure(123, "html_security_challenge", stop_all=True),
                3600,
            )
            self.assertEqual(store.global_wait_remaining(), 3600)
            self.assertFalse(store.is_due(123))

            resumed = ResultStore(output)
            self.assertEqual(resumed.global_wait_remaining(), 3600)

    async def test_terminal_failure_is_not_retried(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            store = ResultStore(Path(temp_dir) / "products.json")
            wait = await store.record_failure(404, "http_404", retryable=False)

            self.assertEqual(wait, 0)
            self.assertFalse(store.is_due(404))

    async def test_stats_are_accumulated_across_resume_runs(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            output = Path(temp_dir) / "products.json"
            store = ResultStore(output)
            await store.save_product(
                {
                    "id": 123,
                    "name": "Test",
                    "url_key": "test-p123",
                    "price": 100,
                    "description": "Mo ta",
                    "images_url": [],
                }
            )

            first_stats = store.update_stats(65.0)
            self.assertEqual(first_stats["total_time_formatted"], "00:01:05")

            resumed = ResultStore(output)
            second_stats = resumed.update_stats(60.0)
            self.assertEqual(second_stats["total_products_saved"], 1)
            self.assertEqual(second_stats["total_time_seconds"], 125.0)
            self.assertEqual(second_stats["total_time_formatted"], "00:02:05")
            self.assertEqual(second_stats["average_speed"], "125.00s / sản phẩm")


if __name__ == "__main__":
    unittest.main()
