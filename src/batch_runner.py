"""Điều phối vòng đời crawl: seed, chạy từng batch, resume và xử lý WAF."""

import asyncio
import logging
import time
from pathlib import Path
from types import SimpleNamespace

from batch_storage import batch_output_path, seed_parts_from_existing_output, summarize_batch
from cooldown import wait_for_cooldown
from fetch_tiki_products import ResultStore, run_product_ids

logger = logging.getLogger("Main")


def _log_status(label: str, index: int, total_batches: int, status: dict[str, int]) -> None:
    """In trạng thái thống nhất để dễ theo dõi log và resume."""
    logger.info(
        "Batch #%04d/%04d %s: total=%s, success=%s, permanent=%s, done=%s, due=%s, pending_retry=%s, cooldown=%ss",
        index, total_batches, label, status["total"], status["success"],
        status["permanent_failed"], status["done"], status["due"],
        status["retry_pending"], status["cooldown"],
    )


async def seed_existing_output(args, product_ids: list[int]) -> bool:
    """Seed output cũ vào parts; trả False nếu chạy ở chế độ seed-only."""
    if args.no_seed_existing:
        return True
    summary = await seed_parts_from_existing_output(
        product_ids, args.batch_size, Path(args.output_dir), Path(args.seed_from_output)
    )
    logger.info(
        "Seed output cũ: thêm %s products, %s permanent fails vào %s batch(es)",
        summary["products_seeded"], summary["failed_seeded"], summary["batches_touched"],
    )
    if args.seed_only:
        logger.info("Hoàn tất seed-only, không gọi API.")
        return False
    return True


def _engine_args(args, output_file: Path) -> SimpleNamespace:
    """Tạo cấu hình tối thiểu truyền cho fetch_tiki_products."""
    return SimpleNamespace(
        output=output_file,
        concurrency=args.concurrency,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        timeout=args.timeout,
        worker_url=args.worker_url,
    )


def _wait_if_needed(args, store: ResultStore, batch_index: int) -> bool:
    """Xử lý cooldown trước request; trả False nếu pipeline phải dừng."""
    cooldown = store.global_wait_remaining()
    if cooldown <= 0:
        return True
    if not args.auto_wait:
        logger.warning("Batch #%04d pending do HTML challenge (%ss), dừng pipeline", batch_index, cooldown)
        return False
    return wait_for_cooldown(store, batch_index, args.auto_wait_interval, args.max_waf_retries)


def _run_one_batch(args, batch: list[int], index: int, total_batches: int) -> bool:
    """Chạy một batch cho đến khi hoàn tất hoặc cần chờ WAF."""
    output_file = batch_output_path(Path(args.output_dir), index)
    while True:
        store = ResultStore(output_file)
        if not _wait_if_needed(args, store, index):
            return False

        before = summarize_batch(batch, output_file)
        _log_status("status trước chạy", index, total_batches, before)
        if before["due"] == 0:
            logger.info("Batch #%04d đã hoàn tất hoặc chỉ còn retry pending: %s", index, output_file)
            return True

        logger.info("Batch #%04d đang hoạt động: xử lý %s ID -> %s", index, before["due"], output_file)
        try:
            asyncio.run(run_product_ids(batch, _engine_args(args, output_file)))
        except Exception as exc:
            logger.error("Batch #%04d gặp ngoại lệ: %s", index, exc)
            if args.auto_wait:
                time.sleep(10)
                continue
            return False

        after = summarize_batch(batch, output_file)
        _log_status("status sau chạy", index, total_batches, after)
        if after["cooldown"] > 0:
            if not args.auto_wait:
                logger.warning("Batch #%04d pending do HTML challenge, dừng pipeline", index)
                return False
            if not wait_for_cooldown(ResultStore(output_file), index, args.auto_wait_interval, args.max_waf_retries):
                return False
            continue
        if after["due"] == 0:
            return True


def run_pipeline(args, product_ids: list[int]) -> None:
    """Chia ID thành batch và chạy tuần tự từ start-batch đến end-batch."""
    from batch_storage import chunk_ids

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    batches = chunk_ids(product_ids, args.batch_size)
    total_batches = len(batches)
    start = args.start_batch
    end = min(args.end_batch or total_batches, total_batches)
    if start > total_batches:
        logger.info("start-batch=%s lớn hơn tổng batch=%s, không có gì để chạy", start, total_batches)
        return

    logger.info(
        "Bắt đầu batch crawl: %s IDs, %s batch, chạy batch %s-%s, size=%s, concurrency=%s, delay=%.1f-%.1fs",
        len(product_ids), total_batches, start, end, args.batch_size,
        args.concurrency, args.delay_min, args.delay_max,
    )
    try:
        for index in range(start, end + 1):
            if not _run_one_batch(args, batches[index - 1], index, total_batches):
                break
    except KeyboardInterrupt:
        logger.warning("Đã dừng bởi người dùng. Chạy lại cùng lệnh để resume.")
