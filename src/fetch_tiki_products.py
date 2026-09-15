"""CLI facade for the Selenium Worker Hybrid product crawler."""

import argparse
import asyncio
from pathlib import Path

from config import TIKI_API_BASE_URL
from crawler_models import FetchResult
from pacer import NaturalPacer
from product_runner import load_product_ids, run, run_product_ids
from result_store import BACKOFF_SECONDS, WAF_BACKOFF_SECONDS, ResultStore
from session_config import load_browser_session, load_worker_urls
from tiki_client import REQUIRED_PRODUCT_FIELDS, fetch_product, parse_retry_after

DEFAULT_INPUT = Path("data/input/product_ids.txt")
DEFAULT_OUTPUT = Path("data/output/concurrency/products_output.json")


def parse_args() -> argparse.Namespace:
    """Parse arguments for crawling a single output file."""
    parser = argparse.ArgumentParser(description="Tiki crawler có checkpoint và retry queue")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=0, help="0 = đọc toàn bộ ID")
    parser.add_argument("--concurrency", type=int, default=20)
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=8.0)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument(
        "--worker-url",
        type=str,
        default=None,
        help=(
            "URL Cloudflare Worker Edge Proxy, nhiều URL phân tách bằng dấu phẩy, "
            "hoặc file .txt chứa danh sách URL (bỏ trống = gọi trực tiếp Tiki API). "
            "Ví dụ: data/input/worker_urls.txt"
        ),
    )
    parser.add_argument(
        "--cookie-file",
        type=Path,
        default=None,
        help="File session/cookie lấy từ Selenium. Ví dụ: data/session/tiki_browser_session.json",
    )
    args = parser.parse_args()
    if not args.input.exists():
        parser.error(f"Không tìm thấy input: {args.input}")
    if args.limit < 0:
        parser.error("--limit phải >= 0")
    if not 1 <= args.concurrency <= 20:
        parser.error("--concurrency phải từ 1 đến 20")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("Cần 0 <= --delay-min <= --delay-max")
    if args.timeout <= 0:
        parser.error("--timeout phải > 0")
    return args


__all__ = [
    "BACKOFF_SECONDS",
    "DEFAULT_INPUT",
    "DEFAULT_OUTPUT",
    "FetchResult",
    "NaturalPacer",
    "REQUIRED_PRODUCT_FIELDS",
    "ResultStore",
    "TIKI_API_BASE_URL",
    "WAF_BACKOFF_SECONDS",
    "fetch_product",
    "load_browser_session",
    "load_product_ids",
    "load_worker_urls",
    "parse_retry_after",
    "run",
    "run_product_ids",
]


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
