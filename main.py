import argparse
import asyncio
import csv
import json
import logging
import sys
from pathlib import Path
from typing import List

from config import (
    BATCH_SIZE,
    DEFAULT_CONCURRENCY,
    DEFAULT_REQUESTS_PER_SECOND,
    INPUT_DIR,
    OUTPUT_DIR,
)
from crawler import TikiAsyncCrawler

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("Main")


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


def main():
    parser = argparse.ArgumentParser(description="Tiki Product Crawler - High Performance & Async")
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
        help=f"Số lượng kết nối đồng thời (mặc định: {DEFAULT_CONCURRENCY})",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=f"Số sản phẩm mỗi file JSON output (mặc định: {BATCH_SIZE})",
    )
    parser.add_argument(
        "--requests-per-second",
        type=float,
        default=DEFAULT_REQUESTS_PER_SECOND,
        help=f"Gioi han request/giay (mac dinh: {DEFAULT_REQUESTS_PER_SECOND})",
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
        default=str(OUTPUT_DIR),
        help=f"Thư mục lưu các file JSON kết quả (mặc định: {OUTPUT_DIR})",
    )
    parser.add_argument(
        "--checkpoint-file",
        type=str,
        default="data/checkpoint.json",
        help="File checkpoint cua dot crawl (mac dinh: data/checkpoint.json)",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Bỏ qua checkpoint cũ và bắt đầu cào mới từ đầu",
    )
    parser.add_argument(
        "--generate-sample",
        action="store_true",
        help="Tạo file danh sách ID mẫu để chạy thử",
    )

    args = parser.parse_args()

    if args.concurrency < 1:
        parser.error("--concurrency phai lon hon 0")
    if args.requests_per_second <= 0:
        parser.error("--requests-per-second phai lon hon 0")
    if args.batch_size < 1:
        parser.error("--batch-size phai lon hon 0")

    sample_input_file = INPUT_DIR / "sample_product_ids.txt"

    # Kiểm tra đường dẫn file input
    if args.input:
        input_path = Path(args.input)
    else:
        # Tìm file trong thư mục input_dir
        input_files = list(INPUT_DIR.glob("*.txt")) + list(INPUT_DIR.glob("*.csv")) + list(INPUT_DIR.glob("*.json"))
        if input_files:
            input_path = input_files[0]
            logger.info(f"Tự động chọn file input: {input_path}")
        elif args.generate_sample or True:
            input_path = sample_input_file
            if not input_path.exists():
                create_sample_ids_file(input_path)

    # Đọc danh sách ID
    product_ids = load_product_ids_from_file(input_path)

    if args.limit > 0:
        product_ids = product_ids[: args.limit]
        logger.info(f"Đã giới hạn danh sách xuống {len(product_ids)} sản phẩm theo tham số --limit")

    if not product_ids:
        logger.error("Danh sách product ID rỗng! Vui lòng cung cấp file input hợp lệ.")
        sys.exit(1)

    # Khởi tạo Crawler
    crawler = TikiAsyncCrawler(
        concurrency=args.concurrency,
        requests_per_second=args.requests_per_second,
        batch_size=args.batch_size,
        output_dir=Path(args.output_dir),
        checkpoint_file=Path(args.checkpoint_file),
        resume=not args.no_resume,
    )

    # Thanh tiến độ
    try:
        from tqdm import tqdm

        pbar = tqdm(total=len(product_ids), desc="Crawling Tiki", unit="product")

        def progress_cb(current, total):
            pbar.update(1)

        close_pbar = pbar.close
    except ImportError:
        def progress_cb(current, total):
            if current % 100 == 0 or current == total:
                percent = (current / total) * 100 if total > 0 else 0
                logger.info(f"Progress: {current}/{total} ({percent:.1f}%)")

        close_pbar = lambda: None

    # Thực thi
    try:
        asyncio.run(crawler.run_async(product_ids, progress_callback=progress_cb))
    except KeyboardInterrupt:
        logger.warning("\nQuá trình cào dữ liệu bị dừng bởi người dùng. Trạng thái đã được lưu vào checkpoint.")
    finally:
        close_pbar()


if __name__ == "__main__":
    main()
