import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from crawler import TikiAsyncCrawler


class FakeResponse:
    def __init__(self, status, body, content_type, extra_headers=None):
        self.status = status
        self.body = body
        self.headers = {"Content-Type": content_type, **(extra_headers or {})}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def text(self):
        return self.body


class FakeSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.call_count = 0

    def get(self, *args, **kwargs):
        self.call_count += 1
        return next(self.responses)


class FetchProductTests(unittest.IsolatedAsyncioTestCase):
    def make_crawler(self, temp_dir):
        crawler = TikiAsyncCrawler(
            concurrency=1,
            requests_per_second=1000,
            output_dir=Path(temp_dir) / "output",
            checkpoint_file=Path(temp_dir) / "checkpoint.json",
            resume=False,
        )
        crawler.pacer.wait = AsyncMock()
        crawler.pacer.cooldown = AsyncMock()
        return crawler

    async def test_html_200_is_retried_then_json_succeeds(self):
        payload = {
            "id": 138083218,
            "name": "San pham",
            "url_key": "san-pham-p138083218",
            "price": 100000,
            "description": "<p>Mo ta&nbsp; sach</p>",
            "images": [{"base_url": "https://example.test/image.jpg"}],
        }
        session = FakeSession([
            FakeResponse(200, "<!DOCTYPE html><title>Security Check</title>", "text/html"),
            FakeResponse(200, json.dumps(payload), "application/json"),
        ])

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "crawler.asyncio.sleep", new=AsyncMock()
        ), patch("crawler.random.uniform", return_value=0):
            crawler = self.make_crawler(temp_dir)
            result = await crawler.fetch_product_aiohttp(
                session, asyncio.Semaphore(1), payload["id"]
            )

        self.assertEqual(result.status, "success")
        self.assertEqual(result.product["description"], "Mo ta sach")
        self.assertEqual(session.call_count, 2)
        crawler.pacer.cooldown.assert_awaited_once_with(60.0)

    async def test_html_200_is_not_treated_as_product(self):
        responses = [
            FakeResponse(200, "<html>Security Check</html>", "text/html")
            for _ in range(6)
        ]
        session = FakeSession(responses)

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "crawler.asyncio.sleep", new=AsyncMock()
        ), patch("crawler.random.uniform", return_value=0):
            crawler = self.make_crawler(temp_dir)
            result = await crawler.fetch_product_aiohttp(
                session, asyncio.Semaphore(1), 123
            )

        self.assertEqual(result.status, "failed")
        self.assertEqual(result.reason, "html_response")
        self.assertEqual(session.call_count, 6)

    async def test_404_is_terminal_without_retry(self):
        session = FakeSession([
            FakeResponse(404, '{"error":"not found"}', "application/json")
        ])

        with tempfile.TemporaryDirectory() as temp_dir:
            crawler = self.make_crawler(temp_dir)
            result = await crawler.fetch_product_aiohttp(
                session, asyncio.Semaphore(1), 404
            )

        self.assertEqual(result.status, "not_found")
        self.assertEqual(session.call_count, 1)


if __name__ == "__main__":
    unittest.main()
