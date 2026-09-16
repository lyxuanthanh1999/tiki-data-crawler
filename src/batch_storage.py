"""Các thao tác file và trạng thái của batch crawl."""

import asyncio
import csv
import json
import logging
from pathlib import Path
from typing import Any

from fetch_tiki_products import ResultStore

logger = logging.getLogger("Main")


def load_product_ids_from_file(file_path: Path) -> list[int]:
    """Đọc ID từ TXT, CSV hoặc JSON, loại trùng nhưng giữ nguyên thứ tự."""
    if not file_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")

    ids: list[int] = []
    suffix = file_path.suffix.lower()
    with file_path.open("r", encoding="utf-8") as file:
        if suffix == ".json":
            data = json.load(file)
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, (int, str)) and str(item).isdigit():
                        ids.append(int(item))
                    elif isinstance(item, dict) and str(item.get("id", "")).isdigit():
                        ids.append(int(item["id"]))
        elif suffix == ".csv":
            for row in csv.reader(file):
                ids.extend(int(value.strip()) for value in row if value.strip().isdigit())
        else:
            ids = [int(line.strip()) for line in file if line.strip().isdigit()]

    unique_ids = list(dict.fromkeys(ids))
    logger.info("Đã đọc %s product IDs từ %s", len(unique_ids), file_path.name)
    return unique_ids


def create_sample_ids_file(file_path: Path, count: int = 20) -> list[int]:
    """Tạo file ID mẫu phục vụ kiểm thử nhanh."""
    sample_ids = [
        138083218, 74070087, 197078650, 273646543, 274291583,
        183884513, 269781898, 274311029, 273919246, 273620247,
        197258327, 274198129, 274198128, 274198127, 184518712,
        273600129, 273600128, 273600127, 273600126, 273600125,
    ]
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text("".join(f"{pid}\n" for pid in sample_ids[:count]), encoding="utf-8")
    logger.info("Đã tạo file ID mẫu tại: %s", file_path)
    return sample_ids[:count]


def chunk_ids(product_ids: list[int], batch_size: int) -> list[list[int]]:
    """Chia danh sách ID thành các batch theo đúng thứ tự ban đầu."""
    return [product_ids[i:i + batch_size] for i in range(0, len(product_ids), batch_size)]


def batch_output_path(output_dir: Path, batch_index: int) -> Path:
    """Sinh đường dẫn output ổn định cho một batch."""
    return output_dir / f"products_part_{batch_index:04d}.json"


def summarize_batch(product_ids: list[int], output_file: Path) -> dict[str, int]:
    """Đếm success, permanent fail, retry pending và ID đến hạn của batch."""
    store = ResultStore(output_file)
    success = len(store.successful_ids)
    permanent_failed = store.permanently_failed_count
    active_ids = [
        pid for pid in product_ids
        if pid not in store.successful_ids and str(pid) not in store.failed_permanent
    ]
    retry_pending = sum(1 for pid in active_ids if str(pid) in store.retry_state and not store.is_due(pid))
    due = sum(1 for pid in active_ids if store.is_due(pid))
    return {
        "total": len(product_ids),
        "success": success,
        "permanent_failed": permanent_failed,
        "done": success + permanent_failed,
        "due": due,
        "retry_pending": retry_pending,
        "cooldown": store.global_wait_remaining(),
    }


def _load_saved_products(output_file: Path) -> list[dict[str, Any]]:
    """Đọc sản phẩm từ JSONL checkpoint hoặc JSON tổng hợp."""
    source = output_file.with_suffix(".jsonl") if output_file.with_suffix(".jsonl").exists() else output_file
    if not source.exists():
        return []
    try:
        if source.suffix == ".jsonl":
            return [json.loads(line) for line in source.read_text(encoding="utf-8").splitlines() if line.strip()]
        data = json.loads(source.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except (OSError, json.JSONDecodeError):
        return []


def _load_failed_permanent(output_file: Path) -> dict[str, dict[str, Any]]:
    """Đọc danh sách lỗi vĩnh viễn từ output cũ."""
    failed_file = output_file.with_suffix(".failed_permanent.json")
    if not failed_file.exists():
        return {}
    try:
        data = json.loads(failed_file.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


async def seed_parts_from_existing_output(
    product_ids: list[int], batch_size: int, output_dir: Path, seed_output: Path
) -> dict[str, int]:
    """Phân bổ kết quả output cũ vào đúng file parts để hỗ trợ resume."""
    products = _load_saved_products(seed_output)
    failed_permanent = _load_failed_permanent(seed_output)
    if not products and not failed_permanent:
        return {"products_seeded": 0, "failed_seeded": 0, "batches_touched": 0}

    id_to_batch = {pid: index // batch_size + 1 for index, pid in enumerate(product_ids)}
    products_by_batch: dict[int, list[dict[str, Any]]] = {}
    for product in products:
        try:
            batch_index = id_to_batch.get(int(product["id"]))
        except (KeyError, TypeError, ValueError):
            continue
        if batch_index is not None:
            products_by_batch.setdefault(batch_index, []).append(product)

    failed_by_batch: dict[int, dict[str, dict[str, Any]]] = {}
    for product_id_text, failure in failed_permanent.items():
        try:
            batch_index = id_to_batch.get(int(product_id_text))
        except ValueError:
            continue
        if batch_index is not None:
            failed_by_batch.setdefault(batch_index, {})[product_id_text] = failure

    products_seeded = failed_seeded = 0
    touched_batches = set(products_by_batch) | set(failed_by_batch)
    for batch_index in sorted(touched_batches):
        store = ResultStore(batch_output_path(output_dir, batch_index))
        for product in products_by_batch.get(batch_index, []):
            if await store.save_product(product):
                products_seeded += 1
        new_failures = {
            pid: failure for pid, failure in failed_by_batch.get(batch_index, {}).items()
            if pid not in store.failed_permanent
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
