import argparse
import asyncio
import csv
import json
import logging
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any, List

from config import (
    BATCH_SIZE,
    DEFAULT_CONCURRENCY,
    INPUT_DIR,
    OUTPUT_DIR,
)
from fetch_tiki_products import ResultStore, run_product_ids

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("Main")
DEFAULT_BATCH_OUTPUT_DIR = OUTPUT_DIR / "concurrency" / "parts"
DEFAULT_SEED_OUTPUT = OUTPUT_DIR / "concurrency" / "products_output.json"


def load_product_ids_from_file(file_path: Path) -> List[int]:
    """Đọc danh sách product_id từ nhiều định dạng file (.txt, .csv, .json)."""
    if not file_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")

    ids: List[int] = []
    suffix = file_path.suffix.lower()

    if suffix == ".json":
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, (int, str)) and str(item).isdigit():
                        ids.append(int(item))
                    elif isinstance(item, dict) and "id" in item:
                        ids.append(int(item["id"]))
    elif suffix == ".csv":
        with open(file_path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            for row in reader:
                for col in row:
                    col_str = col.strip()
                    if col_str.isdigit():
                        ids.append(int(col_str))
    else:  # .txt hoặc định dạng khác
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line_str = line.strip()
                if line_str.isdigit():
                    ids.append(int(line_str))

    # Loại bỏ ID trùng lặp nhưng giữ nguyên thứ tự
    unique_ids = list(dict.fromkeys(ids))
    logger.info(f"Đã đọc {len(unique_ids)} product IDs từ {file_path.name}")
    return unique_ids


def create_sample_ids_file(file_path: Path, count: int = 20) -> List[int]:
    """Tạo danh sách mẫu các ID phổ biến trên Tiki để kiểm thử."""
    sample_ids = [
        138083218, 74070087, 197078650, 273646543, 274291583,
        183884513, 269781898, 274311029, 273919246, 273620247,
        197258327, 274198129, 274198128, 274198127, 184518712,
        273600129, 273600128, 273600127, 273600126, 273600125,
    ]
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with open(file_path, "w", encoding="utf-8") as f:
        for pid in sample_ids[:count]:
            f.write(f"{pid}\n")
    logger.info(f"Đã tạo file ID mẫu tại: {file_path}")
    return sample_ids[:count]


def chunk_ids(product_ids: List[int], batch_size: int) -> list[list[int]]:
    return [
        product_ids[index : index + batch_size]
        for index in range(0, len(product_ids), batch_size)
    ]


def batch_output_path(output_dir: Path, batch_index: int) -> Path:
    return output_dir / f"products_part_{batch_index:04d}.json"


def summarize_batch(product_ids: list[int], output_file: Path) -> dict[str, int]:
    store = ResultStore(output_file)
    success = len(store.successful_ids)
    permanent_failed = store.permanently_failed_count
    retry_pending = sum(
        1
        for product_id in product_ids
        if product_id not in store.successful_ids
        and str(product_id) not in store.failed_permanent
        and str(product_id) in store.retry_state
        and not store.is_due(product_id)
    )
    due = sum(
        1
        for product_id in product_ids
        if product_id not in store.successful_ids
        and str(product_id) not in store.failed_permanent
        and store.is_due(product_id)
    )
    done = success + permanent_failed
    return {
        "total": len(product_ids),
        "success": success,
        "permanent_failed": permanent_failed,
        "done": done,
        "due": due,
        "retry_pending": retry_pending,
        "cooldown": store.global_wait_remaining(),
    }


def load_saved_products(output_file: Path) -> list[dict[str, Any]]:
    source = output_file.with_suffix(".jsonl") if output_file.with_suffix(".jsonl").exists() else output_file
    if not source.exists():
        return []
    if source.suffix == ".jsonl":
        with source.open("r", encoding="utf-8") as file:
            return [json.loads(line) for line in file if line.strip()]
    with source.open("r", encoding="utf-8") as file:
        data = json.load(file)
    return data if isinstance(data, list) else []


def load_failed_permanent(output_file: Path) -> dict[str, dict[str, Any]]:
    failed_file = output_file.with_suffix(".failed_permanent.json")
    if not failed_file.exists():
        return {}
    try:
        with failed_file.open("r", encoding="utf-8") as file:
            data = json.load(file)
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


async def seed_parts_from_existing_output(
    product_ids: list[int],
    batch_size: int,
    output_dir: Path,
    seed_output: Path,
) -> dict[str, int]:
    products = load_saved_products(seed_output)
    failed_permanent = load_failed_permanent(seed_output)
    if not products and not failed_permanent:
        return {"products_seeded": 0, "failed_seeded": 0, "batches_touched": 0}

    id_to_batch = {
        product_id: (index // batch_size) + 1
        for index, product_id in enumerate(product_ids)
    }
    products_by_batch: dict[int, list[dict[str, Any]]] = {}
    for product in products:
        try:
            product_id = int(product["id"])
        except (KeyError, TypeError, ValueError):
            continue
        batch_index = id_to_batch.get(product_id)
        if batch_index is not None:
            products_by_batch.setdefault(batch_index, []).append(product)

    failed_by_batch: dict[int, dict[str, dict[str, Any]]] = {}
    for product_id_text, failure in failed_permanent.items():
        try:
            product_id = int(product_id_text)
        except ValueError:
            continue
        batch_index = id_to_batch.get(product_id)
        if batch_index is not None:
            failed_by_batch.setdefault(batch_index, {})[product_id_text] = failure

    products_seeded = 0
    failed_seeded = 0
    touched_batches = set(products_by_batch) | set(failed_by_batch)
    for batch_index in sorted(touched_batches):
        store = ResultStore(batch_output_path(output_dir, batch_index))
        for product in products_by_batch.get(batch_index, []):
            if await store.save_product(product):
                products_seeded += 1

        new_failures = {
            product_id_text: failure
            for product_id_text, failure in failed_by_batch.get(batch_index, {}).items()
            if product_id_text not in store.failed_permanent
        }
        if new_failures:
            store.failed_permanent.update(new_failures)
            store._write_json_atomic(store.failed_permanent_file, store.failed_permanent)
            failed_seeded += len(new_failures)

    return {
        "products_seeded": products_seeded,
        "failed_seeded": failed_seeded,
        "batches_touched": len(touched_batches),
    }


def wait_for_cooldown(
    store: ResultStore,
    batch_index: int,
    auto_wait_interval: float,
    max_retries: int,
) -> bool:
    """
    Chờ WAF cooldown với Adaptive Stepped Backoff.

    Giai đoạn 1 – Cooldown chính: Ngủ cho đến khi hết thời gian cooldown (thường 3600s).
    Giai đoạn 2 – Adaptive check sau cooldown:
      - Lần check 1-3: Interval ngắn (3-5 phút) để nhanh chóng phát hiện WAF đã clear.
      - Sau 3 lần thất bại: Stepped backoff theo lịch cố định:
          5 → 10 → 15 → 30 → 60 phút (giữ nguyên ở 60 phút mãi).
      - Reset về interval ngắn mỗi lần WAF cooldown mới bắt đầu.

    Trả về True nếu sẵn sàng chạy tiếp, False nếu vượt quá max_retries hoặc bị ngắt.
    """
    # Cấu hình Adaptive Backoff (tính bằng giây)
    ADAPTIVE_SHORT_MIN = 3 * 60    # 3 phút - interval ngắn tối thiểu
    ADAPTIVE_SHORT_MAX = 5 * 60    # 5 phút - interval ngắn tối đa
    ADAPTIVE_LONG_MIN = 5 * 60     # 5 phút - interval dài tối thiểu (sau 3 lần thất bại)
    ADAPTIVE_LONG_MAX = 60 * 60    # 60 phút - giới hạn trên tuyệt đối
    # Lịch stepped backoff (giây): 5m → 10m → 15m → 30m → 60m (giữ nguyên)
    BACKOFF_STEPS = [5 * 60, 10 * 60, 15 * 60, 30 * 60, 60 * 60]
    SHORT_CHECK_LIMIT = 3          # Số lần check ngắn trước khi chuyển sang backoff dài

    waf_retries = 0          # Tổng số lần thử cooldown (cross-cycle)
    adaptive_check = 0       # Số lần check trong giai đoạn adaptive (reset mỗi cycle)
    adaptive_interval = float(ADAPTIVE_SHORT_MIN)  # Interval hiện tại cho adaptive check

    while True:
        cooldown = store.global_wait_remaining()

        # ─── Cooldown chính đã hết → vào giai đoạn adaptive check ───
        if cooldown <= 0:
            if adaptive_check == 0:
                # Lần đầu cooldown kết thúc: báo hiệu và bắt đầu giai đoạn check nhanh
                logger.info(
                    "Batch #%04d: ⏱️ Cooldown WAF chính đã kết thúc! "
                    "Bắt đầu kiểm tra adaptive (lần 1/%d với interval %.0fs)...",
                    batch_index,
                    SHORT_CHECK_LIMIT,
                    adaptive_interval,
                )
                time.sleep(adaptive_interval)
                adaptive_check += 1
                continue  # Quay lại check cooldown

            # Sau lần ngủ adaptive → check lại xem WAF đã thực sự clear chưa
            cooldown_recheck = store.global_wait_remaining()
            if cooldown_recheck <= 0:
                # ✅ WAF đã clear → tiếp tục crawl
                logger.info(
                    "Batch #%04d: ✅ WAF đã clear sau %d lần kiểm tra adaptive! Sẵn sàng chạy tiếp.",
                    batch_index,
                    adaptive_check,
                )
                return True

            # ❌ Vẫn còn WAF → tăng backoff
            waf_retries += 1
            if max_retries > 0 and waf_retries > max_retries:
                logger.error(
                    "Batch #%04d: Đã vượt quá số lần chờ WAF tối đa (%s lần). Dừng batch.",
                    batch_index,
                    max_retries,
                )
                return False

            if adaptive_check <= SHORT_CHECK_LIMIT:
                # Vẫn trong giai đoạn check ngắn (3-5 phút)
                next_interval = ADAPTIVE_SHORT_MIN + (
                    (ADAPTIVE_SHORT_MAX - ADAPTIVE_SHORT_MIN) * adaptive_check / SHORT_CHECK_LIMIT
                )
                logger.warning(
                    "Batch #%04d: ⚠️ WAF vẫn active sau lần kiểm tra %d/%d. "
                    "Tiếp tục check ngắn sau %.0fs (%.1f phút)... (WAF retry #%s)",
                    batch_index,
                    adaptive_check,
                    SHORT_CHECK_LIMIT,
                    next_interval,
                    next_interval / 60,
                    waf_retries if max_retries > 0 else f"{waf_retries}/∞",
                )
                adaptive_interval = next_interval
            else:
                # Chuyển sang stepped backoff: 5 → 10 → 15 → 30 → 60 phút (giữ 60 phút)
                step_idx = min(adaptive_check - SHORT_CHECK_LIMIT - 1, len(BACKOFF_STEPS) - 1)
                long_interval = BACKOFF_STEPS[step_idx]
                logger.warning(
                    "Batch #%04d: 🔴 WAF dai dẳng (check lần %d, bước %d/%d). "
                    "Stepped Backoff: %.0fs (%.0f phút)... (WAF retry #%s)",
                    batch_index,
                    adaptive_check,
                    step_idx + 1,
                    len(BACKOFF_STEPS),
                    long_interval,
                    long_interval / 60,
                    waf_retries if max_retries > 0 else f"{waf_retries}/∞",
                )
                adaptive_interval = long_interval

            time.sleep(adaptive_interval)
            adaptive_check += 1
            continue

        # ─── Cooldown chính vẫn còn → ngủ phần còn lại ───
        waf_retries += 1
        adaptive_check = 0  # Reset adaptive check cho cycle cooldown mới
        adaptive_interval = float(ADAPTIVE_SHORT_MIN)  # Reset interval về ngắn

        if max_retries > 0 and waf_retries > max_retries:
            logger.error(
                "Batch #%04d: Đã vượt quá số lần chờ WAF tối đa (%s lần). Dừng batch.",
                batch_index,
                max_retries,
            )
            return False

        # Ngủ đúng phần còn lại của cooldown chính
        sleep_main = min(cooldown, max(5, int(auto_wait_interval)))
        logger.warning(
            "Batch #%04d: 🛡️ WAF Cooldown đang chạy (còn %ds ~ %.1f phút). "
            "Ngủ %ds rồi kiểm tra lại... (Lần %s/%s)",
            batch_index,
            cooldown,
            cooldown / 60,
            sleep_main,
            waf_retries,
            max_retries if max_retries > 0 else "∞",
        )
        time.sleep(sleep_main)


def main():
    parser = argparse.ArgumentParser(description="Tiki Product Crawler - Batch Orchestrator")
    parser.add_argument(
        "--input",
        type=str,
        default="",
        help="Đường dẫn tới file danh sách product_id (.txt, .csv, .json). Mặc định tìm trong data/input/",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=DEFAULT_CONCURRENCY,
        help=f"Số worker async cho mỗi batch (mặc định: {DEFAULT_CONCURRENCY}, tối đa 20)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Số sản phẩm mỗi file JSON output (mặc định: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Giới hạn số lượng ID cần cào (dùng để test nhanh, 0 = cào toàn bộ)",
    )
    parser.add_argument(
        "--start-batch",
        type=int,
        default=1,
        help="Batch bắt đầu xử lý, đánh số từ 1",
    )
    parser.add_argument(
        "--end-batch",
        type=int,
        default=0,
        help="Batch cuối cùng cần xử lý, 0 = tới batch cuối",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default=str(DEFAULT_BATCH_OUTPUT_DIR),
        help=f"Thư mục lưu các file JSON theo batch (mặc định: {DEFAULT_BATCH_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--delay-min",
        type=float,
        default=2.0,
        help="Delay ngẫu nhiên tối thiểu giữa các request trong engine fetch_tiki_products.py",
    )
    parser.add_argument(
        "--delay-max",
        type=float,
        default=5.0,
        help="Delay ngẫu nhiên tối đa giữa các request trong engine fetch_tiki_products.py",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="Timeout cho mỗi request",
    )
    parser.add_argument(
        "--seed-from-output",
        type=str,
        default=str(DEFAULT_SEED_OUTPUT),
        help=(
            "Import dữ liệu đã crawl từ output cũ vào thư mục parts trước khi chạy "
            f"(mặc định: {DEFAULT_SEED_OUTPUT})"
        ),
    )
    parser.add_argument(
        "--no-seed-existing",
        action="store_true",
        help="Không import dữ liệu từ output cũ vào parts",
    )
    parser.add_argument(
        "--seed-only",
        action="store_true",
        help="Chỉ import dữ liệu output cũ sang parts rồi dừng, không gọi API",
    )
    parser.add_argument(
        "--generate-sample",
        action="store_true",
        help="Tạo file danh sách ID mẫu để chạy thử",
    )
    parser.add_argument(
        "--worker-url",
        type=str,
        default=None,
        help=(
            "URL Cloudflare Worker Edge Proxy (bỏ trống = gọi trực tiếp Tiki API). "
            "Ví dụ: https://tiki-proxy-worker.tyanh185.workers.dev"
        ),
    )
    parser.add_argument(
        "--auto-wait",
        dest="auto_wait",
        action="store_true",
        default=True,
        help="Tự động ngủ chờ khi gặp WAF/cooldown và tự cào tiếp (mặc định: bật)",
    )
    parser.add_argument(
        "--no-auto-wait",
        dest="auto_wait",
        action="store_false",
        help="Tắt tự động ngủ chờ khi gặp WAF (thoát tiến trình ngay)",
    )
    parser.add_argument(
        "--auto-wait-interval",
        type=float,
        default=30.0,
        help="Khoảng thời gian kiểm tra lại cooldown khi đang Auto-Wait (giây, mặc định: 30.0)",
    )
    parser.add_argument(
        "--max-waf-retries",
        type=int,
        default=0,
        help="Số lần Auto-Wait tối đa cho một batch trước khi dừng (0 = không giới hạn, mặc định: 0)",
    )

    args = parser.parse_args()

    if not 1 <= args.concurrency <= 20:
        parser.error("--concurrency phải từ 1 đến 20")
    if args.batch_size < 1:
        parser.error("--batch-size phải lớn hơn 0")
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

    sample_input_file = INPUT_DIR / "sample_product_ids.txt"

    # Kiểm tra đường dẫn file input
    if args.input:
        input_path = Path(args.input)
    else:
        default_input_file = INPUT_DIR / "product_ids.txt"
        if default_input_file.exists():
            input_path = default_input_file
            logger.info(f"Tự động chọn file input mặc định: {input_path}")
        else:
            input_files = sorted(
                list(INPUT_DIR.glob("*.txt"))
                + list(INPUT_DIR.glob("*.csv"))
                + list(INPUT_DIR.glob("*.json"))
            )
            if input_files:
                input_path = input_files[0]
                logger.info(f"Tự động chọn file input: {input_path}")
            elif args.generate_sample:
                input_path = sample_input_file
                if not input_path.exists():
                    create_sample_ids_file(input_path)
            else:
                parser.error("Không tìm thấy input. Hãy truyền --input hoặc dùng --generate-sample.")

    # Đọc danh sách ID
    product_ids = load_product_ids_from_file(input_path)

    if args.limit > 0:
        product_ids = product_ids[: args.limit]
        logger.info(f"Đã giới hạn danh sách xuống {len(product_ids)} sản phẩm theo tham số --limit")

    if not product_ids:
        logger.error("Danh sách product ID rỗng! Vui lòng cung cấp file input hợp lệ.")
        sys.exit(1)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    batches = chunk_ids(product_ids, args.batch_size)
    total_batches = len(batches)
    start_batch = args.start_batch
    end_batch = args.end_batch or total_batches
    if start_batch > total_batches:
        logger.info(
            "start-batch=%s lớn hơn tổng số batch=%s, không có gì để chạy.",
            start_batch,
            total_batches,
        )
        return
    end_batch = min(end_batch, total_batches)
    if not args.no_seed_existing:
        seed_summary = asyncio.run(
            seed_parts_from_existing_output(
                product_ids,
                args.batch_size,
                output_dir,
                Path(args.seed_from_output),
            )
        )
        logger.info(
            "Seed từ output cũ: thêm %s products, %s permanent fails vào %s batch(es).",
            seed_summary["products_seeded"],
            seed_summary["failed_seeded"],
            seed_summary["batches_touched"],
        )
        if args.seed_only:
            logger.info("Hoàn tất seed-only, không gọi API.")
            return
    logger.info(
        (
            "Bắt đầu batch crawl: %s IDs, tổng %s batch, chạy batch %s-%s, "
            "batch_size=%s, concurrency=%s, delay=%.1f-%.1fs"
        ),
        len(product_ids),
        total_batches,
        start_batch,
        end_batch,
        args.batch_size,
        args.concurrency,
        args.delay_min,
        args.delay_max,
    )

    try:
        stop_pipeline = False
        for index in range(start_batch, end_batch + 1):
            if stop_pipeline:
                break
            batch = batches[index - 1]
            output_file = batch_output_path(output_dir, index)

            while True:
                store = ResultStore(output_file)
                cooldown = store.global_wait_remaining()
                if cooldown > 0:
                    if not args.auto_wait:
                        logger.warning(
                            "Batch #%04d pending do HTML challenge/cooldown (%ss). Dừng main.py. Output: %s",
                            index,
                            cooldown,
                            output_file,
                        )
                        stop_pipeline = True
                        break
                    if not wait_for_cooldown(store, index, args.auto_wait_interval, args.max_waf_retries):
                        stop_pipeline = True
                        break

                current_status = summarize_batch(batch, output_file)
                logger.info(
                    (
                        "Batch #%04d/%04d status: "
                        "total=%s, success=%s, permanent=%s, done=%s, "
                        "due=%s, pending_retry=%s, cooldown=%ss"
                    ),
                    index,
                    total_batches,
                    current_status["total"],
                    current_status["success"],
                    current_status["permanent_failed"],
                    current_status["done"],
                    current_status["due"],
                    current_status["retry_pending"],
                    current_status["cooldown"],
                )

                if current_status["due"] == 0:
                    logger.info(
                        (
                            "Batch #%04d không có ID đến hạn xử lý. "
                            "Hoàn tất batch. done=%s/%s, pending_retry=%s, output=%s"
                        ),
                        index,
                        current_status["done"],
                        current_status["total"],
                        current_status["retry_pending"],
                        output_file,
                    )
                    break

                logger.info(
                    "Batch #%04d đang hoạt động: xử lý %s ID đến hạn -> %s",
                    index,
                    current_status["due"],
                    output_file,
                )
                batch_args = SimpleNamespace(
                    output=output_file,
                    concurrency=args.concurrency,
                    delay_min=args.delay_min,
                    delay_max=args.delay_max,
                    timeout=args.timeout,
                    worker_url=args.worker_url,
                )
                try:
                    asyncio.run(run_product_ids(batch, batch_args))
                except Exception as exc:
                    logger.error("Batch #%04d gặp ngoại lệ trong quá trình chạy: %s", index, exc)
                    if args.auto_wait:
                        logger.info("Chờ 10s trước khi thử lại batch #%04d...", index)
                        time.sleep(10)
                        continue
                    else:
                        stop_pipeline = True
                        break

                after = summarize_batch(batch, output_file)
                logger.info(
                    (
                        "Batch #%04d/%04d status sau chạy: "
                        "success=%s, permanent=%s, done=%s/%s, "
                        "due=%s, pending_retry=%s, cooldown=%ss"
                    ),
                    index,
                    total_batches,
                    after["success"],
                    after["permanent_failed"],
                    after["done"],
                    after["total"],
                    after["due"],
                    after["retry_pending"],
                    after["cooldown"],
                )

                if after["cooldown"] > 0:
                    if not args.auto_wait:
                        logger.warning(
                            "Batch #%04d vừa bị pending do HTML challenge (%ss). Dừng main.py.",
                            index,
                            after["cooldown"],
                        )
                        stop_pipeline = True
                        break
                    logger.warning("Batch #%04d gặp WAF challenge, tiến hành Auto-Wait...", index)
                    store = ResultStore(output_file)
                    if not wait_for_cooldown(store, index, args.auto_wait_interval, args.max_waf_retries):
                        stop_pipeline = True
                        break
                    # Cooldown đã hết -> vòng lặp while True tiếp tục chạy nốt ID còn lại của batch này
                    continue

                # Nếu sau khi chạy không bị cooldown và không còn due -> batch đã hoàn thành
                if after["due"] == 0:
                    break
    except KeyboardInterrupt:
        logger.warning("\nQuá trình cào dữ liệu bị dừng bởi người dùng. Chạy lại cùng lệnh để resume.")


if __name__ == "__main__":
    main()
