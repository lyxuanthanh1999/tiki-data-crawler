#!/usr/bin/env python3
"""
Tiki Product Data - Master Batch Parts Merger & Export Pipeline
Hợp nhất dữ liệu toàn diện từ cả 3 giai đoạn:
  - Phase 1: data/output/concurrency/parts (200 batches)
  - Phase 2: data/output/retry_pass2/parts (95 batches)
  - Phase 3: data/output/retry_pass3/parts (84 batches)
Khử trùng lặp ID, tự động cập nhật danh sách 404 và xuất file JSON, JSONL, CSV.
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Set

DEFAULT_PARTS_DIRS = [
    Path("data/output/concurrency/parts"),
    Path("data/output/retry_pass2/parts"),
    Path("data/output/retry_pass3/parts"),
]
DEFAULT_INPUT_FILE = Path("data/input/product_ids.txt")
DEFAULT_OUTPUT_JSON = Path("data/output/concurrency/products_output.json")
DEFAULT_OUTPUT_CSV = Path("data/output/all_products.csv")


def parse_args():
    parser = argparse.ArgumentParser(description="Hợp nhất các file batch parts từ Phase 1, 2, 3 thành file tổng hợp")
    parser.add_argument(
        "--output-json",
        type=Path,
        default=DEFAULT_OUTPUT_JSON,
        help="Đường dẫn file JSON tổng hợp (mặc định: data/output/concurrency/products_output.json)",
    )
    parser.add_argument(
        "--export-csv",
        type=Path,
        default=DEFAULT_OUTPUT_CSV,
        help="Đường dẫn file CSV xuất ra (mặc định: data/output/all_products.csv)",
    )
    parser.add_argument(
        "--input-ids",
        type=Path,
        default=DEFAULT_INPUT_FILE,
        help="Đường dẫn file 200,000 ID gốc (mặc định: data/input/product_ids.txt)",
    )
    return parser.parse_args()


def load_products_from_dir(parts_dir: Path) -> Dict[int, Dict[str, Any]]:
    """Quét tất cả file .jsonl và .json trong thư mục parts để lấy sản phẩm sạch."""
    products = {}
    if not parts_dir.exists():
        return products

    # 1. Đọc từ .jsonl trước (ưu tiên stream append mới nhất)
    for jsonl_file in sorted(parts_dir.glob("products_part_*.jsonl")):
        try:
            with open(jsonl_file, "r", encoding="utf-8") as f:
                for line in f:
                    line_str = line.strip()
                    if line_str:
                        p = json.loads(line_str)
                        pid = p.get("id")
                        if pid:
                            products[pid] = p
        except Exception:
            pass

    # 2. Đọc bổ sung từ .json nếu chưa có
    for json_file in sorted(parts_dir.glob("products_part_*.json")):
        if json_file.name.endswith(".failed_permanent.json") or json_file.name.endswith(".retry.json") or json_file.name.endswith(".stats.json"):
            continue
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    for p in data:
                        pid = p.get("id")
                        if pid and pid not in products:
                            products[pid] = p
        except Exception:
            pass

    return products


def main():
    args = parse_args()
    start_time = time.time()

    print("=" * 70)
    print("📦 BẮT ĐẦU MASTER MERGE: HỢP NHẤT TOÀN DIỆN PHASE 1, 2, 3")
    print("=" * 70)

    all_products: Dict[int, Dict[str, Any]] = {}
    phase_stats = []
    total_raw_products = 0  # Tổng sản phẩm trước khi dedup

    # Quét lần lượt 3 thư mục Phase 1, 2, 3
    for idx, p_dir in enumerate(DEFAULT_PARTS_DIRS, 1):
        if p_dir.exists():
            prods = load_products_from_dir(p_dir)
            new_added = 0
            overwritten = 0
            for pid, pdata in prods.items():
                if pid not in all_products:
                    all_products[pid] = pdata
                    new_added += 1
                else:
                    all_products[pid] = pdata  # ghi đè dữ liệu mới nhất
                    overwritten += 1
            total_raw_products += len(prods)
            phase_stats.append((f"Phase {idx}", p_dir, len(prods), new_added, overwritten))
            dup_tag = f" | 🔁 {overwritten:,} trùng (ghi đè)" if overwritten > 0 else " | ✅ Không trùng"
            print(f"  🔹 [Phase {idx}] {p_dir}")
            print(f"       📥 Tổng load : {len(prods):,} | ➕ Thêm mới: {new_added:,}{dup_tag}")
        else:
            print(f"  🔹 [Phase {idx}] {p_dir} -> (Chưa khởi tạo)")

    total_clean_products = len(all_products)
    total_cross_phase_dups = total_raw_products - total_clean_products

    # Đọc tập 200,000 ID gốc để đối chiếu chính xác số lượng lỗi 404
    total_input_ids = 200000
    all_input_set = set()
    if args.input_ids.exists():
        with open(args.input_ids, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.isdigit():
                    all_input_set.add(int(line))
        total_input_ids = len(all_input_set)

    # Tính toán danh sách lỗi 404 thực tế
    saved_pids = set(all_products.keys())
    failed_pids = sorted(list(all_input_set - saved_pids)) if all_input_set else []
    total_failed = len(failed_pids)

    print("-" * 70)
    print(f"📥 Tổng sản phẩm load từ tất cả parts : {total_raw_products:,}")
    if total_cross_phase_dups > 0:
        print(f"🔁 ID trùng cross-phase (đã loại bỏ) : {total_cross_phase_dups:,}")
    else:
        print(f"✅ ID trùng cross-phase               : 0 (Các phase hoàn toàn tách biệt)")
    print(f"🌟 Tổng sản phẩm sạch sau dedup        : {total_clean_products:,}")
    print(f"❌ Số lỗi 404 thực tế còn lại          : {total_failed:,}")
    print(f"📦 Tổng ID đã giải quyết               : {total_clean_products + total_failed:,} / {total_input_ids:,}")
    print("-" * 70)

    # 1. Ghi file JSON tổng hợp (Atomic Write)
    all_products_list = sorted(all_products.values(), key=lambda x: x["id"])
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    temp_json = args.output_json.with_suffix(".tmp")
    with open(temp_json, "w", encoding="utf-8") as f:
        json.dump(all_products_list, f, ensure_ascii=False, indent=2)
    temp_json.replace(args.output_json)

    # 2. Ghi file JSONL tổng hợp
    output_jsonl = args.output_json.with_suffix(".jsonl")
    with open(output_jsonl, "w", encoding="utf-8") as f:
        for p in all_products_list:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    # 3. Ghi file Failed Permanent tổng hợp
    failed_dict = {str(pid): {"reason": "http_404"} for pid in failed_pids}
    output_failed = args.output_json.with_name(args.output_json.stem + ".failed_permanent.json")
    with open(output_failed, "w", encoding="utf-8") as f:
        json.dump(failed_dict, f, ensure_ascii=False, indent=2)

    # 4. Ghi file progress.txt tổng hợp
    output_progress = args.output_json.with_name(args.output_json.stem + ".progress.txt")
    with open(output_progress, "w", encoding="utf-8") as f:
        for pid in sorted(saved_pids):
            f.write(f"{pid}\n")

    # 5. Xuất file CSV
    csv_info = ""
    if args.export_csv:
        args.export_csv.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = ["id", "name", "price", "url_key", "images_url", "description"]
        with open(args.export_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for p in all_products_list:
                row = dict(p)
                if isinstance(row.get("images_url"), list):
                    row["images_url"] = ";".join(row["images_url"])
                writer.writerow(row)
        csv_size_mb = os.path.getsize(args.export_csv) / (1024 * 1024)
        csv_info = f"\n  📊 File CSV           : {args.export_csv} ({csv_size_mb:.1f} MB)"

    elapsed = time.time() - start_time
    json_size_mb = os.path.getsize(args.output_json) / (1024 * 1024)

    print("======================================================================")
    print(f"🎉 HỢP NHẤT TOÀN DIỆN THÀNH CÔNG TRONG {elapsed:.1f} GIÂY!")
    print(f"  ✅ Tổng sản phẩm sạch  : {total_clean_products:,} ({total_clean_products/total_input_ids*100:.2f}%)")
    print(f"  ❌ Lỗi vĩnh viễn (404): {total_failed:,} ({total_failed/total_input_ids*100:.2f}%)")
    print(f"  📄 File JSON tổng hợp : {args.output_json} ({json_size_mb:.1f} MB)")
    print(f"  📄 File JSONL tổng hợp: {output_jsonl}")
    print(f"  🚫 File 404 tổng hợp  : {output_failed}")
    print(f"  📝 File Progress ID   : {output_progress}{csv_info}")
    print("======================================================================")


if __name__ == "__main__":
    main()
