"""CLI chính của Tiki batch crawler.

`main.py` chỉ khởi động pipeline. Logic được tách thành:
- `cli.py`: tham số và validation.
- `batch_storage.py`: input, output parts, summary và seed/resume.
- `cooldown.py`: chờ WAF cooldown.
- `batch_runner.py`: điều phối từng batch.

Chạy từ thư mục gốc:
    python src/main.py --help
"""

import logging
import sys

from batch_runner import run_pipeline, seed_existing_output
from batch_storage import (
    batch_output_path,
    chunk_ids,
    create_sample_ids_file,
    load_product_ids_from_file,
    seed_parts_from_existing_output,
    summarize_batch,
)
from cli import parse_args, resolve_input_path
from cooldown import wait_for_cooldown

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def main() -> None:
    """Điểm vào CLI: đọc input, seed checkpoint cũ rồi chạy batch crawler."""
    args = parse_args()
    try:
        input_path = resolve_input_path(args)
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    if args.generate_sample and not input_path.exists():
        create_sample_ids_file(input_path)
    product_ids = load_product_ids_from_file(input_path)
    if args.limit:
        product_ids = product_ids[:args.limit]
    if not product_ids:
        raise SystemExit("Danh sách product ID rỗng. Vui lòng kiểm tra file input.")

    import asyncio
    if not asyncio.run(seed_existing_output(args, product_ids)):
        return
    run_pipeline(args, product_ids)


if __name__ == "__main__":
    main()
