"""Batch slicing, status summaries, and seeding helpers."""

import csv
import json
from pathlib import Path
from typing import Any

from result_store import ResultStore


def load_product_ids_from_file(file_path: Path) -> list[int]:
    """Load product IDs from txt/csv/json and preserve the first occurrence."""
    if not file_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")

    ids: list[int] = []
    suffix = file_path.suffix.lower()
    if suffix == ".json":
        ids.extend(_load_ids_from_json(file_path))
    elif suffix == ".csv":
        ids.extend(_load_ids_from_csv(file_path))
    else:
        ids.extend(_load_ids_from_text(file_path))
    return list(dict.fromkeys(ids))


def _load_ids_from_json(file_path: Path) -> list[int]:
    with file_path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    ids: list[int] = []
    if isinstance(data, list):
        for item in data:
            if isinstance(item, (int, str)) and str(item).isdigit():
                ids.append(int(item))
            elif isinstance(item, dict) and "id" in item:
                ids.append(int(item["id"]))
    return ids


def _load_ids_from_csv(file_path: Path) -> list[int]:
    ids: list[int] = []
    with file_path.open("r", encoding="utf-8") as file:
        for row in csv.reader(file):
            ids.extend(int(col.strip()) for col in row if col.strip().isdigit())
    return ids


def _load_ids_from_text(file_path: Path) -> list[int]:
    with file_path.open("r", encoding="utf-8") as file:
        return [int(line.strip()) for line in file if line.strip().isdigit()]


def create_sample_ids_file(file_path: Path, count: int = 20) -> list[int]:
    """Create a small deterministic product ID file for local smoke tests."""
    sample_ids = [
        138083218, 74070087, 197078650, 273646543, 274291583,
        183884513, 269781898, 274311029, 273919246, 273620247,
        197258327, 274198129, 274198128, 274198127, 184518712,
        273600129, 273600128, 273600127, 273600126, 273600125,
    ]
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as file:
        for product_id in sample_ids[:count]:
            file.write(f"{product_id}\n")
    return sample_ids[:count]


def chunk_ids(product_ids: list[int], batch_size: int) -> list[list[int]]:
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
    """Import old consolidated output into the current per-batch stores."""
    products = load_saved_products(seed_output)
    failed_permanent = load_failed_permanent(seed_output)
    if not products and not failed_permanent:
        return {"products_seeded": 0, "failed_seeded": 0, "batches_touched": 0}

    id_to_batch = {
        product_id: (index // batch_size) + 1
        for index, product_id in enumerate(product_ids)
    }
    products_by_batch = _group_products_by_batch(products, id_to_batch)
    failed_by_batch = _group_failures_by_batch(failed_permanent, id_to_batch)

    products_seeded = 0
    failed_seeded = 0
    for batch_index in sorted(set(products_by_batch) | set(failed_by_batch)):
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
        "batches_touched": len(set(products_by_batch) | set(failed_by_batch)),
    }


def _group_products_by_batch(
    products: list[dict[str, Any]],
    id_to_batch: dict[int, int],
) -> dict[int, list[dict[str, Any]]]:
    grouped: dict[int, list[dict[str, Any]]] = {}
    for product in products:
        try:
            batch_index = id_to_batch.get(int(product["id"]))
        except (KeyError, TypeError, ValueError):
            continue
        if batch_index is not None:
            grouped.setdefault(batch_index, []).append(product)
    return grouped


def _group_failures_by_batch(
    failed_permanent: dict[str, dict[str, Any]],
    id_to_batch: dict[int, int],
) -> dict[int, dict[str, dict[str, Any]]]:
    grouped: dict[int, dict[str, dict[str, Any]]] = {}
    for product_id_text, failure in failed_permanent.items():
        try:
            batch_index = id_to_batch.get(int(product_id_text))
        except ValueError:
            continue
        if batch_index is not None:
            grouped.setdefault(batch_index, {})[product_id_text] = failure
    return grouped
