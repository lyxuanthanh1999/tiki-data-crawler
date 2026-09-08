#!/usr/bin/env python3
"""
Tiki Product Data - Batch Parts Merger & Export Pipeline
Hợp nhất toàn bộ 200 file batch (.json / .jsonl) thành file tổng hợp duy nhất.
"""

import argparse
import csv
import glob
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Set


def parse_args():
    parser = argparse.ArgumentParser(description="Hợp nhất các file batch parts thành file tổng hợp")
    parser.add_argument(
        "--parts-dir",
        type=Path,
        default=Path("data/output/concurrency/parts"),
        help="Thư mục chứa các file parts (mặc định: data/output/concurrency/parts)",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("data/output/concurrency/products_output.json"),
        help="Đường dẫn file JSON tổng hợp (mặc định: data/output/concurrency/products_output.json)",
    )
    parser.add_argument(
        "--export-csv",
        type=Path,
        default=None,
        help="Đường dẫn file CSV xuất ra (tùy chọn, vd: data/output/all_products.csv)",
    )
    return parser.parse_args()


def load_products_from_part(part_file: Path) -> List[Dict[str, Any]]:
    """Đọc sản phẩm từ file batch (ưu tiên .jsonl nếu có để lấy dữ liệu mới nhất)."""
    jsonl_path = part_file.with_suffix(".jsonl")
    products = []
    if jsonl_path.exists():
        try:
            with open(jsonl_path, "r", encoding="utf-8") as f:
                for line in f:
                    line_str = line.strip()
                    if line_str:
                        products.append(json.loads(line_str))
            return products
        except Exception:
            pass

    if part_file.exists():
        try:
            with open(part_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, list):
                    return data
        except Exception:
            pass

    return []


def load_failed_permanent(part_file: Path) -> Dict[str, Any]:
    perm_path = part_file.with_name(part_file.stem + ".failed_permanent.json")
    if perm_path.exists():
        try:
            with open(perm_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {}


def main():
    args = parse_args()
    start_time = time.time()

    if not args.parts_dir.exists():
        print(f"❌ Thư mục không tồn tại: {args.parts_dir}")
        sys.exit(1)

    import re
    batch_indices = set()
    for f in args.parts_dir.glob("products_part_*.*"):
        m = re.search(r"products_part_(\d+)\.", f.name)
        if m:
            batch_indices.add(int(m.group(1)))

    sorted_indices = sorted(batch_indices)
    print("======================================================================")
    print(f"📦 Bắt đầu hợp nhất {len(sorted_indices)} batches từ: {args.parts_dir}")
    print("======================================================================")

    all_products: List[Dict[str, Any]] = []
    seen_ids: Set[int] = set()
    all_failed_perm: Dict[str, Any] = {}

    for index, batch_idx in enumerate(sorted_indices, 1):
        part_file = args.parts_dir / f"products_part_{batch_idx:04d}.json"
        # 1. Đọc sản phẩm thành công
        prods = load_products_from_part(part_file)
        for p in prods:
            pid = p.get("id")
            if pid is not None and pid not in seen_ids:
                seen_ids.add(pid)
                all_products.append(p)

        # 2. Đọc lỗi vĩnh viễn
        failed_data = load_failed_permanent(part_file)
        all_failed_perm.update(failed_data)

        if index % 20 == 0 or index == len(sorted_indices):
            print(f"  🔹 Đã duyệt {index}/{len(sorted_indices)} batches -> {len(all_products):,} sản phẩm, {len(all_failed_perm):,} 404")

    # Ghi file JSON tổng hợp (Atomic Write)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    temp_json = args.output_json.with_suffix(".tmp")
    with open(temp_json, "w", encoding="utf-8") as f:
        json.dump(all_products, f, ensure_ascii=False, indent=2)
    temp_json.replace(args.output_json)

    # Ghi file JSONL tổng hợp
    output_jsonl = args.output_json.with_suffix(".jsonl")
    with open(output_jsonl, "w", encoding="utf-8") as f:
        for p in all_products:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    # Ghi file Failed Permanent tổng hợp
    output_failed = args.output_json.with_name(args.output_json.stem + ".failed_permanent.json")
    with open(output_failed, "w", encoding="utf-8") as f:
        json.dump(all_failed_perm, f, ensure_ascii=False, indent=2)

    # Ghi file progress.txt tổng hợp
    output_progress = args.output_json.with_name(args.output_json.stem + ".progress.txt")
    with open(output_progress, "w", encoding="utf-8") as f:
        for pid in seen_ids:
            f.write(f"{pid}\n")

    # Xuất CSV nếu được yêu cầu
    csv_info = ""
    if args.export_csv:
        args.export_csv.parent.mkdir(parents=True, exist_ok=True)
        fieldnames = ["id", "name", "price", "url_key", "images_url", "description"]
        with open(args.export_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for p in all_products:
                row = dict(p)
                if isinstance(row.get("images_url"), list):
                    row["images_url"] = ";".join(row["images_url"])
                writer.writerow(row)
        csv_size_mb = os.path.getsize(args.export_csv) / (1024 * 1024)
        csv_info = f"\n  📊 File CSV           : {args.export_csv} ({csv_size_mb:.1f} MB)"

    elapsed = time.time() - start_time
    json_size_mb = os.path.getsize(args.output_json) / (1024 * 1024)
    total_resolved = len(all_products) + len(all_failed_perm)

    print("\n======================================================================")
    print(f"🎉 HỢP NHẤT THÀNH CÔNG TRONG {elapsed:.1f} GIÂY!")
    print(f"  ✅ Tổng sản phẩm sạch  : {len(all_products):,} ({len(all_products)/200000*100:.2f}%)")
    print(f"  ❌ Lỗi vĩnh viễn (404): {len(all_failed_perm):,} ({len(all_failed_perm)/200000*100:.2f}%)")
    print(f"  📦 Tổng ID đã giải quyết: {total_resolved:,} / 200,000 ({total_resolved/200000*100:.2f}%)")
    print(f"  📄 File JSON tổng hợp : {args.output_json} ({json_size_mb:.1f} MB)")
    print(f"  📄 File JSONL tổng hợp: {output_jsonl}")
    print(f"  🚫 File 404 tổng hợp  : {output_failed}")
    print(f"  📝 File Progress ID   : {output_progress}{csv_info}")
    print("======================================================================")


if __name__ == "__main__":
    main()
