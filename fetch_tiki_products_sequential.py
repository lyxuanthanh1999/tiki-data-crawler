"""Sequential Tiki product crawler with lower delay and separated error ID files."""

import argparse
import asyncio
import json
import os
import random
import time
from pathlib import Path
from typing import Any

import aiohttp

from cleaner import extract_product_fields
from config import DEFAULT_HEADERS, TIKI_API_BASE_URL
from fetch_tiki_products import ResultStore, load_product_ids

DEFAULT_INPUT = Path("data/input/product_ids.txt")
DEFAULT_OUTPUT = Path("data/output/squential/products_sequential_output.json")


def append_unique_line(path: Path, key: str, line: str) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_keys: set[str] = set()
    if path.exists():
        with path.open("r", encoding="utf-8") as file:
            existing_keys = {
                row.split("\t", 1)[0].strip()
                for row in file
                if row.strip()
            }
    if key in existing_keys:
        return False
    with path.open("a", encoding="utf-8") as file:
        file.write(line.rstrip("\n") + "\n")
        file.flush()
        os.fsync(file.fileno())
    return True


def validate_required_fields(product: dict[str, Any]) -> list[str]:
    missing: list[str] = []
    if product.get("id") is None:
        missing.append("id")
    if not isinstance(product.get("name"), str) or not product["name"].strip():
        missing.append("name")
    if not isinstance(product.get("url_key"), str) or not product["url_key"].strip():
        missing.append("url_key")
    if product.get("price") is None:
        missing.append("price")
    if not isinstance(product.get("description"), str) or not product["description"].strip():
        missing.append("description")
    images_url = product.get("images_url")
    if not isinstance(images_url, list) or not any(
        isinstance(url, str) and url.strip() for url in images_url
    ):
        missing.append("images_url")
    return missing


async def fetch_one(
    session: aiohttp.ClientSession,
    product_id: int,
) -> tuple[str, dict[str, Any] | None, str]:
    url = f"{TIKI_API_BASE_URL}/{product_id}"
    try:
        async with session.get(url) as response:
            body = await response.text()
            content_type = response.headers.get("Content-Type", "").lower()

            if response.status == 404:
                return "terminal", None, "http_404"
            if response.status in {403, 429}:
                return "retry", None, f"http_{response.status}"
            if response.status >= 500:
                return "retry", None, f"http_{response.status}"
            if response.status != 200:
                return "terminal", None, f"http_{response.status}"
            if "html" in content_type or body.lstrip().startswith("<"):
                return "challenge", None, "html_security_challenge"
            if "json" not in content_type:
                return "retry", None, "invalid_content_type"

            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                return "retry", None, "invalid_json"
            if not isinstance(data, dict) or data.get("id") is None:
                return "terminal", None, "invalid_product_payload"

            product = extract_product_fields(data)
            product["images_url"] = product.pop("images")
            missing_fields = validate_required_fields(product)
            if missing_fields:
                return "incomplete", product, ",".join(missing_fields)
            return "success", product, ""
    except asyncio.TimeoutError:
        return "retry", None, "timeout"
    except aiohttp.ClientError as exc:
        return "retry", None, type(exc).__name__


async def run(args: argparse.Namespace) -> None:
    product_ids = load_product_ids(args.input, args.limit)
    store = ResultStore(args.output)
    html_challenge_file = args.output.with_suffix(".html_challenge_ids.txt")
    incomplete_file = args.output.with_suffix(".incomplete_ids.txt")
    failed_file = args.output.with_suffix(".failed_ids.txt")
    for status_file in (html_challenge_file, incomplete_file, failed_file):
        status_file.parent.mkdir(parents=True, exist_ok=True)
        status_file.touch(exist_ok=True)

    global_wait = store.global_wait_remaining()
    if global_wait:
        print(
            f"Retry queue dang cooldown do HTML challenge. "
            f"Chay lai sau it nhat {global_wait}s."
        )
        return

    pending_ids = [
        product_id
        for product_id in product_ids
        if product_id not in store.successful_ids and store.is_due(product_id)
    ]
    deferred = len(product_ids) - len(store.successful_ids) - len(pending_ids)
    print(
        f"IDs={len(product_ids)}, da thanh cong={len(store.successful_ids)}, "
        f"can xu ly={len(pending_ids)}, dang backoff={max(0, deferred)}"
    )
    if not pending_ids:
        print(f"Khong co ID den han xu ly. Output: {args.output}")
        return

    counters = {
        "success": 0,
        "incomplete": 0,
        "challenge": 0,
        "failed": 0,
    }
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    connector = aiohttp.TCPConnector(limit=1, limit_per_host=1, ttl_dns_cache=300)
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=DEFAULT_HEADERS,
        cookie_jar=aiohttp.CookieJar(),
    ) as session:
        for index, product_id in enumerate(pending_ids, start=1):
            if index > 1:
                await asyncio.sleep(random.uniform(args.delay_min, args.delay_max))

            status, product, reason = await fetch_one(session, product_id)
            if status == "success" and product is not None:
                if await store.save_product(product):
                    counters["success"] += 1
                print(f"[OK] {product_id} -> da luu ngay ({len(store.products)} products)")
                continue

            if status == "incomplete":
                counters["incomplete"] += 1
                append_unique_line(
                    incomplete_file,
                    str(product_id),
                    f"{product_id}\tmissing={reason}",
                )
                print(f"[INCOMPLETE] {product_id}: thieu {reason}")
                continue

            wait_seconds = await store.record_failure(
                product_id,
                reason,
                retryable=status != "terminal",
                stop_all=status == "challenge",
            )
            if status == "challenge":
                counters["challenge"] += 1
                append_unique_line(html_challenge_file, str(product_id), str(product_id))
                print(
                    f"[STOP] {product_id}: HTML challenge; "
                    f"da ghi vao {html_challenge_file}; retry sau {wait_seconds}s"
                )
                break

            counters["failed"] += 1
            attempts = store.retry_state.get(str(product_id), {}).get("attempts", 0)
            append_unique_line(
                failed_file,
                str(product_id),
                f"{product_id}\treason={reason}\tattempts={attempts}",
            )
            if wait_seconds:
                print(f"[RETRY] {product_id}: {reason}; hen lai sau {wait_seconds}s")
            else:
                print(f"[FAILED] {product_id}: {reason}; het retry")

    print(
        "Ket thuc: "
        f"moi={counters['success']}, "
        f"incomplete={counters['incomplete']}, "
        f"html_challenge={counters['challenge']}, "
        f"loi={counters['failed']}, "
        f"tong da luu={len(store.products)}. Output: {args.output}"
    )
    print(f"HTML challenge IDs: {html_challenge_file}")
    print(f"Incomplete IDs: {incomplete_file}")
    print(f"Failed IDs: {failed_file}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tiki crawler tuan tu, delay thap, tach file ID loi")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=0, help="0 = doc toan bo ID")
    parser.add_argument("--delay-min", type=float, default=0.8)
    parser.add_argument("--delay-max", type=float, default=2.0)
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()
    if not args.input.exists():
        parser.error(f"Khong tim thay input: {args.input}")
    if args.limit < 0:
        parser.error("--limit phai >= 0")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("Can 0 <= --delay-min <= --delay-max")
    if args.timeout <= 0:
        parser.error("--timeout phai > 0")
    return args


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
