import argparse
import asyncio
import csv
import json
import logging
import sys
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
        help=f"Số worker async cho mỗi batch (mặc định: {DEFAULT_CONCURRENCY}, tối đa 10)",
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

    args = parser.parse_args()

    if not 1 <= args.concurrency <= 10:
        parser.error("--concurrency phải từ 1 đến 10")
    if args.batch_size < 1:
        parser.error("--batch-size phải lớn hơn 0")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("Cần 0 <= --delay-min <= --delay-max")
    if args.timeout <= 0:
        parser.error("--timeout phải > 0")
    if args.seed_only and args.no_seed_existing:
        parser.error("--seed-only không dùng chung với --no-seed-existing")

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
        "Bắt đầu batch crawl: %s IDs, %s batch, batch_size=%s, concurrency=%s, delay=%.1f-%.1fs",
        len(product_ids),
        len(batches),
        args.batch_size,
        args.concurrency,
        args.delay_min,
        args.delay_max,
    )

    try:
        for index, batch in enumerate(batches, start=1):
            output_file = batch_output_path(output_dir, index)
            before = summarize_batch(batch, output_file)
            logger.info(
                (
                    "Batch #%04d/%04d status trước chạy: "
                    "total=%s, success=%s, permanent=%s, done=%s, "
                    "due=%s, pending_retry=%s, cooldown=%ss"
                ),
                index,
                len(batches),
                before["total"],
                before["success"],
                before["permanent_failed"],
                before["done"],
                before["due"],
                before["retry_pending"],
                before["cooldown"],
            )
            if before["cooldown"]:
                logger.warning(
                    (
                        "Batch #%04d pending do HTML challenge/cooldown. "
                        "Chạy lại sau ít nhất %ss. Output: %s"
                    ),
                    index,
                    before["cooldown"],
                    output_file,
                )
                break

            if before["due"] == 0:
                logger.info(
                    (
                        "Batch #%04d không có ID đến hạn xử lý. "
                        "Bỏ qua. done=%s/%s, pending_retry=%s, output=%s"
                    ),
                    index,
                    before["done"],
                    before["total"],
                    before["retry_pending"],
                    output_file,
                )
                continue

            logger.info(
                "Batch #%04d đang hoạt động: xử lý %s ID đến hạn -> %s",
                index,
                before["due"],
                output_file,
            )
            batch_args = SimpleNamespace(
                output=output_file,
                concurrency=args.concurrency,
                delay_min=args.delay_min,
                delay_max=args.delay_max,
                timeout=args.timeout,
            )
            asyncio.run(run_product_ids(batch, batch_args))

            after = summarize_batch(batch, output_file)
            logger.info(
                (
                    "Batch #%04d/%04d status sau chạy: "
                    "success=%s, permanent=%s, done=%s/%s, "
                    "due=%s, pending_retry=%s, cooldown=%ss"
                ),
                index,
                len(batches),
                after["success"],
                after["permanent_failed"],
                after["done"],
                after["total"],
                after["due"],
                after["retry_pending"],
                after["cooldown"],
            )
            if after["cooldown"]:
                logger.warning(
                    (
                        "Batch #%04d vừa bị pending do HTML challenge. "
                        "Dừng toàn bộ main.py; chạy lại sau ít nhất %ss để resume."
                    ),
                    index,
                    after["cooldown"],
                )
                break
    except KeyboardInterrupt:
        logger.warning("\nQuá trình cào dữ liệu bị dừng bởi người dùng. Chạy lại cùng lệnh để resume.")


if __name__ == "__main__":
    main()
