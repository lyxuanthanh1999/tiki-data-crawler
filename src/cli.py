"""Khai báo và kiểm tra tham số dòng lệnh cho batch crawler."""

import argparse
from pathlib import Path

from config import BATCH_SIZE, DEFAULT_CONCURRENCY, INPUT_DIR, OUTPUT_DIR

DEFAULT_BATCH_OUTPUT_DIR = OUTPUT_DIR / "concurrency" / "parts"
DEFAULT_SEED_OUTPUT = OUTPUT_DIR / "concurrency" / "products_output.json"


def build_parser() -> argparse.ArgumentParser:
    """Tạo parser; chạy `python src/main.py --help` để xem toàn bộ lệnh."""
    parser = argparse.ArgumentParser(description="Tiki Product Crawler - Batch Orchestrator")
    parser.add_argument("--input", default="", help="File product ID (.txt, .csv, .json)")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Số worker async mỗi batch (1-20)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE, help="Số ID mỗi file output")
    parser.add_argument("--limit", type=int, default=0, help="Giới hạn ID để test; 0 = toàn bộ")
    parser.add_argument("--start-batch", type=int, default=1, help="Batch bắt đầu, đánh số từ 1")
    parser.add_argument("--end-batch", type=int, default=0, help="Batch cuối; 0 = batch cuối cùng")
    parser.add_argument("--output-dir", default=str(DEFAULT_BATCH_OUTPUT_DIR), help="Thư mục output parts")
    parser.add_argument("--delay-min", type=float, default=2.0, help="Delay thấp nhất giữa request (giây)")
    parser.add_argument("--delay-max", type=float, default=5.0, help="Delay cao nhất giữa request (giây)")
    parser.add_argument("--timeout", type=float, default=20.0, help="Timeout mỗi request (giây)")
    parser.add_argument("--seed-from-output", default=str(DEFAULT_SEED_OUTPUT), help="Output cũ dùng để seed sang parts")
    parser.add_argument("--no-seed-existing", action="store_true", help="Không seed output cũ")
    parser.add_argument("--seed-only", action="store_true", help="Chỉ seed output cũ rồi dừng")
    parser.add_argument("--generate-sample", action="store_true", help="Tạo file ID mẫu để test")
    parser.add_argument("--worker-url", default=None, help="URL Cloudflare Worker; bỏ trống để gọi Tiki trực tiếp")
    parser.add_argument("--auto-wait", dest="auto_wait", action="store_true", default=True, help="Tự chờ WAF (mặc định bật)")
    parser.add_argument("--no-auto-wait", dest="auto_wait", action="store_false", help="Dừng khi gặp WAF")
    parser.add_argument("--auto-wait-interval", type=float, default=30.0, help="Khoảng kiểm tra cooldown (giây)")
    parser.add_argument("--max-waf-retries", type=int, default=0, help="Số lần chờ WAF; 0 = không giới hạn")
    return parser


def parse_args() -> argparse.Namespace:
    """Parse và validate CLI; các giá trị lỗi dừng ngay với hướng dẫn cụ thể."""
    parser = build_parser()
    args = parser.parse_args()
    if not 1 <= args.concurrency <= 20:
        parser.error("--concurrency phải từ 1 đến 20")
    if args.batch_size < 1 or args.limit < 0 or args.start_batch < 1:
        parser.error("--batch-size >= 1, --limit >= 0, --start-batch >= 1")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("Cần 0 <= --delay-min <= --delay-max")
    if args.timeout <= 0 or args.auto_wait_interval <= 0 or args.max_waf_retries < 0:
        parser.error("timeout/auto-wait-interval phải > 0 và max-waf-retries >= 0")
    if args.end_batch and args.end_batch < args.start_batch:
        parser.error("--end-batch phải >= --start-batch hoặc bằng 0")
    if args.seed_only and args.no_seed_existing:
        parser.error("--seed-only không dùng chung với --no-seed-existing")
    return args


def resolve_input_path(args: argparse.Namespace) -> Path:
    """Chọn input theo --input, file mặc định, file đầu tiên hoặc tạo sample."""
    if args.input:
        return Path(args.input)
    default = INPUT_DIR / "product_ids.txt"
    if default.exists():
        return default
    candidates = sorted(INPUT_DIR.glob("*.txt")) + sorted(INPUT_DIR.glob("*.csv")) + sorted(INPUT_DIR.glob("*.json"))
    if candidates:
        return candidates[0]
    if args.generate_sample:
        return INPUT_DIR / "sample_product_ids.txt"
    raise ValueError("Không tìm thấy input. Hãy truyền --input hoặc dùng --generate-sample.")
