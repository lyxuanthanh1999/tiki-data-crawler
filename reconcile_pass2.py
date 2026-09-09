#!/usr/bin/env python3
"""
Pipeline đối chiếu và hợp nhất sản phẩm 'vớt' được từ Phase 2 vào Dataset chính Phase 1.
"""

import argparse
import csv
import json
import os
import sys
import time
from pathlib import Path

RETRY_PARTS_DIR = Path("data/output/retry_pass2/parts")
MAIN_OUTPUT_JSON = Path("data/output/concurrency/products_output.json")
MAIN_OUTPUT_JSONL = Path("data/output/concurrency/products_output.jsonl")
MAIN_OUTPUT_CSV = Path("data/output/all_products.csv")
MAIN_FAILED_JSON = Path("data/output/concurrency/products_output.failed_permanent.json")
MAIN_PROGRESS_TXT = Path("data/output/concurrency/products_output.progress.txt")


def load_recovered_products():
    recovered = {}
    if not RETRY_PARTS_DIR.exists():
        return recovered

    for jsonl_file in sorted(RETRY_PARTS_DIR.glob("products_part_*.jsonl")):
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    product = json.loads(line)
                    pid = product.get("id")
                    if pid:
                        recovered[pid] = product
                except Exception:
                    pass
    return recovered


def main():
    print("=" * 70)
    print("🔄 BẮT ĐẦU ĐỐI CHIẾU & HỢP NHẤT DỮ LIỆU PHASE 2 VÀO PHASE 1")
    print("=" * 70)

    recovered = load_recovered_products()
    print(f"🌟 Tổng sản phẩm 'vớt' được từ Phase 2: {len(recovered):,} sản phẩm.")

    if not recovered:
        print("ℹ️ Chưa có sản phẩm mới nào được vớt từ Phase 2.")
        return

    # 1. Đọc Dataset Phase 1
    main_products = {}
    if MAIN_OUTPUT_JSON.exists():
        with open(MAIN_OUTPUT_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
            for p in data:
                main_products[p["id"]] = p

    # 2. Đọc danh sách 404 Phase 1
    failed_perm = {}
    if MAIN_FAILED_JSON.exists():
        with open(MAIN_FAILED_JSON, "r", encoding="utf-8") as f:
            failed_perm = json.load(f)

    new_count = 0
    for pid, pdata in recovered.items():
        if pid not in main_products:
            main_products[pid] = pdata
            new_count += 1
        # Xóa khỏi danh sách 404 nếu đã cứu được
        failed_perm.pop(str(pid), None)

    print(f"✅ Đã bổ sung thêm {new_count:,} sản phẩm mới vào Dataset chính.")
    print(f"📦 Tổng sản phẩm sạch sau khi hợp nhất: {len(main_products):,}")
    print(f"❌ Số lỗi 404 thực tế còn lại: {len(failed_perm):,}")

    # 3. Ghi lại các file tổng hợp
    all_products_list = sorted(main_products.values(), key=lambda x: x["id"])

    # Atomic write JSON
    tmp_json = MAIN_OUTPUT_JSON.with_suffix(".json.tmp")
    with open(tmp_json, "w", encoding="utf-8") as f:
        json.dump(all_products_list, f, ensure_ascii=False, indent=2)
    tmp_json.replace(MAIN_OUTPUT_JSON)

    # Write JSONL
    with open(MAIN_OUTPUT_JSONL, "w", encoding="utf-8") as f:
        for p in all_products_list:
            f.write(json.dumps(p, ensure_ascii=False) + "\n")

    # Write CSV
    if MAIN_OUTPUT_CSV.exists() or True:
        fieldnames = ["id", "name", "price", "url_key", "images_url", "description"]
        with open(MAIN_OUTPUT_CSV, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for p in all_products_list:
                row = dict(p)
                if isinstance(row.get("images_url"), list):
                    row["images_url"] = ";".join(row["images_url"])
                writer.writerow(row)

    # Update failed_permanent.json
    tmp_failed = MAIN_FAILED_JSON.with_suffix(".json.tmp")
    with open(tmp_failed, "w", encoding="utf-8") as f:
        json.dump(failed_perm, f, ensure_ascii=False, indent=2)
    tmp_failed.replace(MAIN_FAILED_JSON)

    # Update progress.txt
    with open(MAIN_PROGRESS_TXT, "w", encoding="utf-8") as f:
        for p in all_products_list:
            f.write(f"{p['id']}\n")

    print("=" * 70)
    print("🎉 HOÀN TẤT ĐỐI CHIẾU & HỢP NHẤT DỮ LIỆU THÀNH CÔNG!")
    print(f"  📄 JSON : {MAIN_OUTPUT_JSON} ({os.path.getsize(MAIN_OUTPUT_JSON)/(1024*1024):.1f} MB)")
    print(f"  📊 CSV  : {MAIN_OUTPUT_CSV} ({os.path.getsize(MAIN_OUTPUT_CSV)/(1024*1024):.1f} MB)")
    print("=" * 70)


if __name__ == "__main__":
    main()
