import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set

from cleaner import extract_product_fields
from config import (
    BATCH_SIZE,
    CHECKPOINT_FILE,
    DEFAULT_CONCURRENCY,
    DEFAULT_HEADERS,
    DEFAULT_REQUESTS_PER_SECOND,
    FAILED_LOG_FILE,
    MAX_RETRIES,
    OUTPUT_DIR,
    REQUEST_TIMEOUT,
    TIKI_API_BASE_URL,
)

logger = logging.getLogger("TikiCrawler")


@dataclass(frozen=True)
class FetchResult:
    status: str
    product: Optional[Dict[str, Any]] = None
    reason: str = ""


class RequestPacer:
    """Gioi han toc do bat dau request va chia se cooldown khi bi rate-limit."""

    def __init__(self, requests_per_second: float):
        self.interval = 1.0 / requests_per_second
        self._next_request_at = 0.0
        self._cooldown_until = 0.0
        self._condition = asyncio.Condition()

    async def wait(self):
        async with self._condition:
            while True:
                now = time.monotonic()
                start_at = max(self._next_request_at, self._cooldown_until)
                delay = start_at - now
                if delay <= 0:
                    self._next_request_at = now + self.interval
                    return
                try:
                    await asyncio.wait_for(self._condition.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass

    async def cooldown(self, seconds: float):
        async with self._condition:
            self._cooldown_until = max(
                self._cooldown_until,
                time.monotonic() + seconds,
            )
            self._condition.notify_all()


class CheckpointManager:
    """Quản lý trạng thái tiến trình cào dữ liệu để có thể resume khi bị ngắt."""

    def __init__(self, filepath: Path = CHECKPOINT_FILE, load_existing: bool = True):
        self.filepath = filepath
        self.processed_ids: Set[int] = set()
        self.last_batch_index: int = 0
        self.total_saved: int = 0
        if load_existing:
            self.load()

    def load(self):
        if self.filepath.exists():
            try:
                with open(self.filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    self.processed_ids = set(data.get("processed_ids", []))
                    self.last_batch_index = data.get("last_batch_index", 0)
                    self.total_saved = data.get("total_saved", 0)
                logger.info(f"Loaded checkpoint: {len(self.processed_ids)} processed IDs, last batch part: {self.last_batch_index}")
            except Exception as e:
                logger.warning(f"Error loading checkpoint: {e}")

    def save(self):
        try:
            self.filepath.parent.mkdir(parents=True, exist_ok=True)
            temp_file = self.filepath.with_suffix(self.filepath.suffix + ".tmp")
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "processed_ids": list(self.processed_ids),
                        "last_batch_index": self.last_batch_index,
                        "total_saved": self.total_saved,
                        "timestamp": time.time(),
                    },
                    f,
                    indent=2,
                )
            temp_file.replace(self.filepath)
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")


class TikiAsyncCrawler:
    """Crawler bất đồng bộ hiệu năng cao cho Tiki API."""

    def __init__(
        self,
        concurrency: int = DEFAULT_CONCURRENCY,
        requests_per_second: float = DEFAULT_REQUESTS_PER_SECOND,
        batch_size: int = BATCH_SIZE,
        output_dir: Path = OUTPUT_DIR,
        checkpoint_file: Path = CHECKPOINT_FILE,
        resume: bool = True,
    ):
        self.concurrency = concurrency
        self.requests_per_second = requests_per_second
        self.batch_size = batch_size
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint = CheckpointManager(checkpoint_file, load_existing=resume)
        self.pacer = RequestPacer(requests_per_second)
        self.failed_ids: List[int] = []
        self.failure_reasons: Dict[str, int] = {}
        self.current_buffer: List[Dict[str, Any]] = []

    def _save_batch(self, items: List[Dict[str, Any]]):
        if not items:
            return
        self.checkpoint.last_batch_index += 1
        batch_num = self.checkpoint.last_batch_index
        filename = self.output_dir / f"tiki_products_part_{batch_num:04d}.json"

        with open(filename, "w", encoding="utf-8") as f:
            json.dump(items, f, ensure_ascii=False, indent=2)

        self.checkpoint.total_saved += len(items)
        self.checkpoint.save()
        logger.info(f"💾 [Batch #{batch_num:04d}] Saved {len(items)} products to {filename.name} (Total: {self.checkpoint.total_saved})")

    async def fetch_product_aiohttp(
        self,
        session: Any,
        semaphore: asyncio.Semaphore,
        product_id: int,
    ) -> FetchResult:
        url = f"{TIKI_API_BASE_URL}/{product_id}"
        headers = DEFAULT_HEADERS.copy()

        for attempt in range(1, MAX_RETRIES + 1):
            retry_after = 0.0
            await self.pacer.wait()
            try:
                async with semaphore:
                    async with session.get(url, headers=headers, timeout=REQUEST_TIMEOUT) as response:
                        content_type = response.headers.get("Content-Type", "").lower()
                        if response.status == 200:
                            body = await response.text()
                            if "html" in content_type or body.lstrip().startswith("<"):
                                reason = "html_response"
                            elif "json" not in content_type:
                                reason = "invalid_content_type"
                            else:
                                try:
                                    data = json.loads(body)
                                except json.JSONDecodeError:
                                    reason = "invalid_json"
                                else:
                                    product = extract_product_fields(data)
                                    if product.get("id") is None:
                                        reason = "invalid_product_payload"
                                    else:
                                        return FetchResult("success", product)
                        elif response.status == 404:
                            return FetchResult("not_found", reason="http_404")
                        elif response.status == 429:
                            reason = "http_429"
                            try:
                                retry_after = float(response.headers.get("Retry-After", "0"))
                            except ValueError:
                                retry_after = 0.0
                        elif 500 <= response.status < 600:
                            reason = f"http_{response.status}"
                        else:
                            return FetchResult("failed", reason=f"http_{response.status}")
            except (asyncio.TimeoutError, OSError) as exc:
                reason = type(exc).__name__
            except Exception as exc:
                reason = type(exc).__name__

            if attempt < MAX_RETRIES:
                sleep_time = max(retry_after, min(30.0, 2 ** (attempt - 1))) + random.uniform(0.25, 1.0)
                if reason == "html_response":
                    # BytePlus tra HTML 200 khi WAF challenge; dung ca hang doi.
                    await self.pacer.cooldown(min(300.0, 60.0 * attempt))
                elif reason == "http_429":
                    await self.pacer.cooldown(sleep_time)
                logger.warning(
                    "Retry ID %s (%s), lan %s/%s sau %.2fs",
                    product_id,
                    reason,
                    attempt,
                    MAX_RETRIES,
                    sleep_time,
                )
                await asyncio.sleep(sleep_time)

        return FetchResult("failed", reason=reason)

    async def run_async(
        self,
        product_ids: List[int],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ):
        try:
            import aiohttp
        except ImportError:
            logger.warning("aiohttp is not installed. Falling back to multi-threaded crawler.")
            self.run_threaded(product_ids, progress_callback)
            return

        # Lọc ra các ID chưa được cào
        ids_to_crawl = [pid for pid in product_ids if pid not in self.checkpoint.processed_ids]
        total = len(ids_to_crawl)
        logger.info(
            "Starting crawl: %s products, concurrency=%s, max_rate=%.1f req/s",
            total,
            self.concurrency,
            self.requests_per_second,
        )

        semaphore = asyncio.Semaphore(self.concurrency)
        connector = aiohttp.TCPConnector(
            limit=self.concurrency + 20,
            limit_per_host=self.concurrency,
            ttl_dns_cache=300,
            enable_cleanup_closed=True,
            ssl=False,
        )

        completed_count = 0
        start_time = time.time()

        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async def worker(pid: int):
                    nonlocal completed_count
                    result = await self.fetch_product_aiohttp(session, semaphore, pid)
                    completed_count += 1

                    if result.status == "success":
                        self.checkpoint.processed_ids.add(pid)
                        self.current_buffer.append(result.product)
                        if len(self.current_buffer) >= self.batch_size:
                            batch_to_save = self.current_buffer[: self.batch_size]
                            self.current_buffer = self.current_buffer[self.batch_size :]
                            self._save_batch(batch_to_save)
                    elif result.status == "not_found":
                        self.checkpoint.processed_ids.add(pid)
                        self.failed_ids.append(pid)
                        self.failure_reasons[result.reason] = self.failure_reasons.get(result.reason, 0) + 1
                    else:
                        self.failed_ids.append(pid)
                        self.failure_reasons[result.reason] = self.failure_reasons.get(result.reason, 0) + 1

                    if progress_callback:
                        progress_callback(completed_count, total)

                # Chia nho task de tranh tao 200k coroutine cung luc.
                chunk_size = 5000
                for i in range(0, len(ids_to_crawl), chunk_size):
                    chunk = ids_to_crawl[i : i + chunk_size]
                    tasks = [worker(pid) for pid in chunk]
                    await asyncio.gather(*tasks)
        finally:
            if self.current_buffer:
                self._save_batch(self.current_buffer)
                self.current_buffer = []
            else:
                self.checkpoint.save()
            self._record_failed_ids()
        elapsed = time.time() - start_time
        speed = completed_count / elapsed if elapsed > 0 else 0
        logger.info(f"✨ Completed crawl in {elapsed:.2f}s (~{speed:.1f} req/s). Total saved: {self.checkpoint.total_saved}")
        if self.failure_reasons:
            logger.info("Failure summary: %s", self.failure_reasons)

    def run_threaded(
        self,
        product_ids: List[int],
        progress_callback: Optional[Callable[[int, int], None]] = None,
    ):
        """Fallback chạy đa luồng bằng thư viện chuẩn (urllib + ThreadPoolExecutor)."""
        import urllib.request
        from concurrent.futures import ThreadPoolExecutor, as_completed

        ids_to_crawl = [pid for pid in product_ids if pid not in self.checkpoint.processed_ids]
        total = len(ids_to_crawl)
        logger.info(f"🚀 [Fallback Mode] Starting multi-thread crawl for {total} products (threads={self.concurrency})")

        completed_count = 0
        start_time = time.time()

        def fetch_sync(pid: int) -> Optional[Dict[str, Any]]:
            url = f"{TIKI_API_BASE_URL}/{pid}"
            req = urllib.request.Request(url, headers=DEFAULT_HEADERS)
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
                        if resp.status == 200:
                            data = json.loads(resp.read().decode("utf-8"))
                            return extract_product_fields(data)
                        elif resp.status == 404:
                            return None
                except Exception:
                    time.sleep(0.5 * attempt)
            return None

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            future_to_id = {executor.submit(fetch_sync, pid): pid for pid in ids_to_crawl}
            for future in as_completed(future_to_id):
                pid = future_to_id[future]
                completed_count += 1
                self.checkpoint.processed_ids.add(pid)
                try:
                    res = future.result()
                    if res:
                        self.current_buffer.append(res)
                        if len(self.current_buffer) >= self.batch_size:
                            batch_to_save = self.current_buffer[: self.batch_size]
                            self.current_buffer = self.current_buffer[self.batch_size :]
                            self._save_batch(batch_to_save)
                    else:
                        self.failed_ids.append(pid)
                except Exception:
                    self.failed_ids.append(pid)

                if progress_callback:
                    progress_callback(completed_count, total)

        if self.current_buffer:
            self._save_batch(self.current_buffer)
            self.current_buffer = []

        self._record_failed_ids()
        elapsed = time.time() - start_time
        logger.info(f"✨ Completed in {elapsed:.2f}s. Total saved: {self.checkpoint.total_saved}")

    def _record_failed_ids(self):
        if self.failed_ids:
            try:
                with open(FAILED_LOG_FILE, "a", encoding="utf-8") as f:
                    for pid in self.failed_ids:
                        f.write(f"{pid}\n")
                logger.info(f"Logged {len(self.failed_ids)} 404/failed IDs to {FAILED_LOG_FILE.name}")
            except Exception as e:
                logger.error(f"Error logging failed IDs: {e}")
