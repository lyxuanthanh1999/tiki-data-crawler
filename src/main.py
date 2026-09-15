"""Batch orchestrator for the Selenium Worker Hybrid crawler."""

import argparse
import asyncio
import logging
import os
import sys
import time
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(__file__))

from batching import (  # noqa: E402
    batch_output_path,
    chunk_ids,
    create_sample_ids_file,
    load_product_ids_from_file,
    seed_parts_from_existing_output,
    summarize_batch,
)
from config import BATCH_SIZE, DEFAULT_CONCURRENCY, INPUT_DIR, OUTPUT_DIR  # noqa: E402
from result_store import ResultStore  # noqa: E402
from product_runner import run_product_ids  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("Main")

DEFAULT_BATCH_OUTPUT_DIR = OUTPUT_DIR / "concurrency" / "parts"
DEFAULT_SEED_OUTPUT = OUTPUT_DIR / "concurrency" / "products_output.json"


def wait_for_cooldown(
    store: ResultStore,
    batch_index: int,
    auto_wait_interval: float,
    max_retries: int,
) -> bool:
    """
    Wait until a batch-level WAF cooldown clears.

    The first phase follows the persisted cooldown in .retry.json. After that,
    the function performs a short adaptive wait before letting the batch retry.
    """
    waf_retries = 0
    adaptive_checks = 0
    short_check_limit = 3
    short_interval = float(auto_wait_interval)

    while True:
        cooldown = store.global_wait_remaining()
        if cooldown <= 0:
            if adaptive_checks == 0:
                logger.info(
                    "Batch #%04d: Cooldown WAF đã hết, kiểm tra lại sau %.0fs.",
                    batch_index,
                    short_interval,
                )
                time.sleep(short_interval)
                adaptive_checks += 1
                continue
            logger.info(
                "Batch #%04d: WAF đã clear sau %d lần kiểm tra.",
                batch_index,
                adaptive_checks,
            )
            return True

        waf_retries += 1
        adaptive_checks = 0
        if max_retries > 0 and waf_retries > max_retries:
            logger.error(
                "Batch #%04d: vượt quá số lần chờ WAF tối đa (%s).",
                batch_index,
                max_retries,
            )
            return False

        sleep_seconds = min(cooldown, max(5, int(auto_wait_interval)))
        logger.warning(
            "Batch #%04d: WAF cooldown còn %ds, ngủ %ds rồi kiểm tra lại. (Lần %s/%s)",
            batch_index,
            cooldown,
            sleep_seconds,
            waf_retries,
            max_retries if max_retries > 0 else "∞",
        )
        time.sleep(sleep_seconds)

        # Avoid tight loops if another process keeps extending cooldown.
        if waf_retries >= short_check_limit and cooldown <= sleep_seconds:
            time.sleep(short_interval)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Tiki Product Crawler - Batch Orchestrator")
    parser.add_argument("--input", type=str, default="", help="File product_id input")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--start-batch", type=int, default=1)
    parser.add_argument("--end-batch", type=int, default=0)
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_BATCH_OUTPUT_DIR))
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=5.0)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--seed-from-output", type=str, default=str(DEFAULT_SEED_OUTPUT))
    parser.add_argument("--no-seed-existing", action="store_true")
    parser.add_argument("--seed-only", action="store_true")
    parser.add_argument("--generate-sample", action="store_true")
    parser.add_argument(
        "--worker-url",
        type=str,
        default=None,
        help="Một Worker URL, nhiều URL, hoặc file .txt như data/input/worker_urls.txt",
    )
    parser.add_argument("--cookie-file", type=Path, default=None)
    parser.add_argument("--auto-wait", dest="auto_wait", action="store_true", default=True)
    parser.add_argument("--no-auto-wait", dest="auto_wait", action="store_false")
    parser.add_argument("--auto-wait-interval", type=float, default=30.0)
    parser.add_argument("--max-waf-retries", type=int, default=0)
    args = parser.parse_args()
    validate_args(parser, args)
    return args


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Fail fast on invalid crawler settings."""
    if not 1 <= args.concurrency <= 20:
        parser.error("--concurrency phải từ 1 đến 20")
    if args.batch_size < 1:
        parser.error("--batch-size phải lớn hơn 0")
    if args.limit < 0:
        parser.error("--limit phải >= 0")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("Cần 0 <= --delay-min <= --delay-max")
    if args.timeout <= 0:
        parser.error("--timeout phải > 0")
    if args.auto_wait_interval <= 0:
        parser.error("--auto-wait-interval phải > 0")
    if args.max_waf_retries < 0:
        parser.error("--max-waf-retries phải >= 0")
    if args.seed_only and args.no_seed_existing:
        parser.error("--seed-only không dùng chung với --no-seed-existing")
    if args.start_batch < 1:
        parser.error("--start-batch phải >= 1")
    if args.end_batch and args.end_batch < args.start_batch:
        parser.error("--end-batch phải >= --start-batch hoặc bằng 0")


def resolve_input_path(args: argparse.Namespace) -> Path:
    """Resolve explicit input, default product_ids.txt, or a generated sample file."""
    if args.input:
        return Path(args.input)

    default_input = INPUT_DIR / "product_ids.txt"
    if default_input.exists():
        logger.info("Tự động chọn file input mặc định: %s", default_input)
        return default_input

    candidates = sorted(
        list(INPUT_DIR.glob("*.txt"))
        + list(INPUT_DIR.glob("*.csv"))
        + list(INPUT_DIR.glob("*.json"))
    )
    if candidates:
        logger.info("Tự động chọn file input: %s", candidates[0])
        return candidates[0]

    if args.generate_sample:
        sample_file = INPUT_DIR / "sample_product_ids.txt"
        create_sample_ids_file(sample_file)
        logger.info("Đã tạo file ID mẫu tại: %s", sample_file)
        return sample_file

    raise FileNotFoundError("Không tìm thấy input. Hãy truyền --input hoặc dùng --generate-sample.")


def load_run_ids(args: argparse.Namespace) -> tuple[Path, list[int]]:
    """Load product IDs and apply the optional test limit."""
    input_path = resolve_input_path(args)
    product_ids = load_product_ids_from_file(input_path)
    logger.info("Đã đọc %s product IDs từ %s", len(product_ids), input_path.name)

    if args.limit > 0:
        product_ids = product_ids[: args.limit]
        logger.info("Đã giới hạn danh sách xuống %s sản phẩm theo --limit", len(product_ids))
    if not product_ids:
        raise ValueError("Danh sách product ID rỗng.")
    return input_path, product_ids


def prepare_batches(args: argparse.Namespace, product_ids: list[int]) -> tuple[Path, list[list[int]], int, int]:
    """Create output directory and calculate selected batch range."""
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    batches = chunk_ids(product_ids, args.batch_size)
    start_batch = args.start_batch
    end_batch = min(args.end_batch or len(batches), len(batches))
    return output_dir, batches, start_batch, end_batch


def seed_existing_output(args: argparse.Namespace, product_ids: list[int], output_dir: Path) -> bool:
    """Seed old consolidated output into part files. Return True when seed-only should stop."""
    if args.no_seed_existing:
        return False
    summary = asyncio.run(
        seed_parts_from_existing_output(
            product_ids,
            args.batch_size,
            output_dir,
            Path(args.seed_from_output),
        )
    )
    logger.info(
        "Seed từ output cũ: thêm %s products, %s permanent fails vào %s batch(es).",
        summary["products_seeded"],
        summary["failed_seeded"],
        summary["batches_touched"],
    )
    if args.seed_only:
        logger.info("Hoàn tất seed-only, không gọi API.")
        return True
    return False


def run_batches(args: argparse.Namespace, batches: list[list[int]], output_dir: Path, start_batch: int, end_batch: int) -> None:
    """Run selected batches in order, staying on the current batch until it finishes."""
    total_batches = len(batches)
    logger.info(
        "Bắt đầu batch crawl: tổng %s batch, chạy batch %s-%s, batch_size=%s, concurrency=%s, delay=%.1f-%.1fs",
        total_batches,
        start_batch,
        end_batch,
        args.batch_size,
        args.concurrency,
        args.delay_min,
        args.delay_max,
    )

    for index in range(start_batch, end_batch + 1):
        if not run_one_batch_until_done(args, batches[index - 1], output_dir, index, total_batches):
            break


def run_one_batch_until_done(
    args: argparse.Namespace,
    batch: list[int],
    output_dir: Path,
    index: int,
    total_batches: int,
) -> bool:
    """Run or wait one batch until complete. Return False when the pipeline must stop."""
    output_file = batch_output_path(output_dir, index)
    while True:
        store = ResultStore(output_file)
        cooldown = store.global_wait_remaining()
        if cooldown > 0 and not handle_cooldown(args, store, index, cooldown, output_file):
            return False

        current = summarize_batch(batch, output_file)
        log_batch_status(index, total_batches, current)
        if current["due"] == 0:
            logger.info(
                "Batch #%04d hoàn tất. done=%s/%s, pending_retry=%s, output=%s",
                index,
                current["done"],
                current["total"],
                current["retry_pending"],
                output_file,
            )
            return True

        logger.info("Batch #%04d đang hoạt động: xử lý %s ID -> %s", index, current["due"], output_file)
        run_batch_fetch(args, batch, output_file, index)

        after = summarize_batch(batch, output_file)
        log_batch_status(index, total_batches, after, suffix="sau chạy")
        if after["cooldown"] > 0:
            store = ResultStore(output_file)
            if not handle_cooldown(args, store, index, after["cooldown"], output_file):
                return False
            continue
        if after["due"] == 0:
            return True


def handle_cooldown(
    args: argparse.Namespace,
    store: ResultStore,
    index: int,
    cooldown: int,
    output_file: Path,
) -> bool:
    """Either wait through WAF cooldown or stop, depending on CLI options."""
    if not args.auto_wait:
        logger.warning(
            "Batch #%04d pending do HTML challenge/cooldown (%ss). Dừng main.py. Output: %s",
            index,
            cooldown,
            output_file,
        )
        return False
    logger.warning("Batch #%04d gặp WAF challenge, tiến hành Auto-Wait...", index)
    return wait_for_cooldown(store, index, args.auto_wait_interval, args.max_waf_retries)


def run_batch_fetch(args: argparse.Namespace, batch: list[int], output_file: Path, index: int) -> None:
    """Call the async product runner for one batch and recover transient exceptions."""
    batch_args = SimpleNamespace(
        output=output_file,
        concurrency=args.concurrency,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        timeout=args.timeout,
        worker_url=args.worker_url,
        cookie_file=args.cookie_file,
    )
    try:
        asyncio.run(run_product_ids(batch, batch_args))
    except Exception as exc:
        logger.error("Batch #%04d gặp ngoại lệ: %s", index, exc)
        if args.auto_wait:
            logger.info("Chờ 10s trước khi thử lại batch #%04d...", index)
            time.sleep(10)
        else:
            raise


def log_batch_status(index: int, total_batches: int, status: dict[str, int], suffix: str = "") -> None:
    label = f" status {suffix}" if suffix else " status"
    logger.info(
        "Batch #%04d/%04d%s: total=%s, success=%s, permanent=%s, done=%s, due=%s, pending_retry=%s, cooldown=%ss",
        index,
        total_batches,
        label,
        status["total"],
        status["success"],
        status["permanent_failed"],
        status["done"],
        status["due"],
        status["retry_pending"],
        status["cooldown"],
    )


def main() -> None:
    args = parse_args()
    try:
        _, product_ids = load_run_ids(args)
        output_dir, batches, start_batch, end_batch = prepare_batches(args, product_ids)
        if start_batch > len(batches):
            logger.info("start-batch=%s lớn hơn tổng số batch=%s, không có gì để chạy.", start_batch, len(batches))
            return
        if seed_existing_output(args, product_ids, output_dir):
            return
        run_batches(args, batches, output_dir, start_batch, end_batch)
    except KeyboardInterrupt:
        logger.warning("\nQuá trình cào dữ liệu bị dừng. Chạy lại cùng lệnh để resume.")
    except (FileNotFoundError, ValueError) as exc:
        logger.error("%s", exc)
        sys.exit(1)


if __name__ == "__main__":
    main()
