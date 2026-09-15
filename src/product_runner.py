"""Async worker queue for fetching one output batch."""

import argparse
import asyncio
import time
from pathlib import Path

import aiohttp

from config import TIKI_API_BASE_URL
from pacer import NaturalPacer
from result_store import ResultStore
from session_config import build_session_headers, load_browser_session, load_worker_urls
from tiki_client import fetch_product


def load_product_ids(input_file: Path, limit: int) -> list[int]:
    """Load unique numeric product IDs while preserving input order."""
    with input_file.open("r", encoding="utf-8") as file:
        ids = [int(line.strip()) for line in file if line.strip().isdigit()]
    ids = list(dict.fromkeys(ids))
    return ids[:limit] if limit else ids


async def run(args: argparse.Namespace) -> None:
    product_ids = load_product_ids(args.input, args.limit)
    await run_product_ids(product_ids, args)


async def run_product_ids(product_ids: list[int], args: argparse.Namespace) -> None:
    """Fetch all due product IDs for a single output file."""
    run_start_time = time.time()
    store = ResultStore(args.output)
    counters = {"success": 0, "failed_temp": 0, "failed_perm": 0}

    def print_summary() -> None:
        stats = store.update_stats(time.time() - run_start_time)
        print(
            f"\nKết thúc lượt chạy:"
            f"\n  ⏱️ Thời gian lượt này  : {stats['last_run_duration']}"
            f"\n  ⌛ Tổng thời gian tích lũy: "
            f"{stats['total_time_human']} ({stats['total_time_formatted']})"
            f"\n  ⚡ Tốc độ trung bình    : {stats['average_speed']}"
            f"\n  ✅ Mới lưu thành công   : {counters['success']}"
            f"\n  ⏳ Lỗi tạm thời (retry): {counters['failed_temp']}"
            f"\n  ❌ Lỗi vĩnh viễn       : "
            f"{counters['failed_perm']} (tổng tích lũy: {store.permanently_failed_count})"
            f"\n  📦 Tổng đã lưu          : {len(store.products)}"
            f"\n  📄 Output               : {args.output}"
            f"\n  📊 Stats                : {store.stats_file}"
            f"\n  🚫 Permanent fails      : {store.failed_permanent_file}"
        )

    global_wait = store.global_wait_remaining()
    if global_wait:
        print(f"Retry queue đang cooldown do HTML challenge. Chạy lại sau ít nhất {global_wait}s.")
        print_summary()
        return

    pending_ids = [
        product_id
        for product_id in product_ids
        if product_id not in store.successful_ids and store.is_due(product_id)
    ]
    deferred_retry = sum(
        1
        for product_id in product_ids
        if product_id not in store.successful_ids
        and str(product_id) not in store.failed_permanent
        and not store.is_due(product_id)
    )
    print(
        f"IDs={len(product_ids)}, đã thành công={len(store.successful_ids)}, "
        f"cần xử lý={len(pending_ids)}, đang backoff={deferred_retry}, "
        f"lỗi vĩnh viễn={store.permanently_failed_count}"
    )
    if not pending_ids:
        print(f"Không có ID đến hạn xử lý. Output: {args.output}")
        print_summary()
        return

    worker_urls = load_worker_urls(getattr(args, "worker_url", None))
    api_base_urls = worker_urls or [TIKI_API_BASE_URL]
    is_worker_mode = bool(worker_urls)
    session_file = getattr(args, "cookie_file", None)
    session_headers, cookie_count = build_session_headers(is_worker_mode, session_file)

    if is_worker_mode:
        endpoint_label = f"☁️  Cloudflare Workers ({len(api_base_urls)}) → {', '.join(api_base_urls)}"
    else:
        endpoint_label = f"🎯 Tiki trực tiếp → {api_base_urls[0]}"
    print(f"🌐 API endpoint: {endpoint_label}")
    if session_file:
        print(f"🍪 Browser session: {session_file} ({cookie_count} cookies)")

    await _run_worker_queue(pending_ids, api_base_urls, session_headers, store, counters, args)
    print_summary()


async def _run_worker_queue(
    pending_ids: list[int],
    api_base_urls: list[str],
    session_headers: dict[str, str],
    store: ResultStore,
    counters: dict[str, int],
    args: argparse.Namespace,
) -> None:
    """Run concurrent workers against the shared ID queue."""
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    connector = aiohttp.TCPConnector(
        limit=args.concurrency,
        limit_per_host=args.concurrency,
        ttl_dns_cache=300,
    )
    pacer = NaturalPacer(args.delay_min, args.delay_max)
    stop_event = asyncio.Event()
    id_queue: asyncio.Queue[int] = asyncio.Queue()
    for product_id in pending_ids:
        id_queue.put_nowait(product_id)

    print(f"🚀 Khởi chạy {args.concurrency} worker(s) bất đồng bộ...")
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=session_headers,
        cookie_jar=aiohttp.CookieJar(),
    ) as session:
        tasks = [
            asyncio.create_task(
                _worker(i + 1, id_queue, api_base_urls, session, pacer, stop_event, store, counters)
            )
            for i in range(args.concurrency)
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


async def _worker(
    worker_id: int,
    id_queue: asyncio.Queue[int],
    api_base_urls: list[str],
    session: aiohttp.ClientSession,
    pacer: NaturalPacer,
    stop_event: asyncio.Event,
    store: ResultStore,
    counters: dict[str, int],
) -> None:
    """Fetch IDs until the queue is empty or a global stop signal appears."""
    tag = f"[Worker {worker_id}]"
    api_base_url = api_base_urls[(worker_id - 1) % len(api_base_urls)]
    while not stop_event.is_set():
        try:
            product_id = id_queue.get_nowait()
        except asyncio.QueueEmpty:
            return

        result = await fetch_product(session, pacer, product_id, api_base_url)
        if result.status == "success":
            if await store.save_product(result.product):
                counters["success"] += 1
            print(f"{tag} [OK] {product_id} -> đã lưu ngay ({len(store.products)} products)")
            continue

        wait_seconds = await store.record_failure(
            product_id,
            result.reason,
            retryable=result.status != "terminal",
            stop_all=result.status in {"challenge", "quota_exceeded"},
            server_retry_after=result.retry_after,
        )
        if result.status == "terminal" or (result.status == "retry" and wait_seconds == 0):
            counters["failed_perm"] += 1
        else:
            counters["failed_temp"] += 1

        if result.status == "quota_exceeded":
            stop_event.set()
            print(_quota_message(tag))
            return
        if result.status == "challenge":
            stop_event.set()
            print(
                f"{tag} [STOP] {product_id}: BytePlus HTML challenge. "
                f"ID được hẹn retry sau {wait_seconds}s; dừng cả lượt chạy."
            )
            return
        if wait_seconds:
            src = f" (Retry-After: {result.retry_after}s)" if result.retry_after else ""
            print(f"{tag} [RETRY] {product_id}: {result.reason}{src}; hẹn lại sau {wait_seconds}s")
        else:
            print(f"{tag} [FAILED] {product_id}: {result.reason}; đã hết retry → ghi permanent")


def _quota_message(tag: str) -> str:
    return (
        f"\n🛑 ====================================================================\n"
        f"🛑 [THÔNG BÁO] ĐÃ CHẠM HẠN MỨC 100,000 REQUESTS/NGÀY CỦA CLOUDFLARE!\n"
        f"🛑 Tiến trình tại {tag} đã ngắt an toàn và đưa vào trạng thái PENDING.\n"
        f"🛑 Không mất dữ liệu! Sang ngày mới (sau 07:00 sáng) chỉ cần bật lại để chạy tiếp.\n"
        f"🛑 ====================================================================\n"
    )


__all__ = [
    "ResultStore",
    "fetch_product",
    "load_browser_session",
    "load_product_ids",
    "load_worker_urls",
    "run",
    "run_product_ids",
]
