#!/usr/bin/env python3
"""
Trích xuất danh sách 83,804 ID lỗi còn lại từ Phase 2 để chuẩn bị cho Phase 3.
Đọc từ data/output/concurrency/products_output.failed_permanent.json
và xuất ra data/input/failed_ids_pass3.txt
"""

import json
import os
import sys
from pathlib import Path

FAILED_PERMANENT_FILE = Path("data/output/concurrency/products_output.failed_permanent.json")
OUTPUT_FILE = Path("data/input/failed_ids_pass3.txt")


def main():
    print("=" * 60)
    print("🔍 Trích xuất danh sách ID lỗi cho Phase 3...")
    print("=" * 60)

    if not FAILED_PERMANENT_FILE.exists():
        print(f"❌ File {FAILED_PERMANENT_FILE} không tồn tại!")
        sys.exit(1)

    with open(FAILED_PERMANENT_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)

    raw_ids = list(data.keys())
    # Lọc và ép kiểu số nguyên để sắp xếp
    valid_ids = []
    for pid in raw_ids:
        if pid.isdigit():
            valid_ids.append(int(pid))

    valid_ids = sorted(list(set(valid_ids)))
    total_count = len(valid_ids)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for pid in valid_ids:
            f.write(f"{pid}\n")

    print(f"✅ Đã trích xuất thành công: {total_count:,} ID")
    print(f"📄 File đầu ra: {OUTPUT_FILE} ({os.path.getsize(OUTPUT_FILE)/(1024):.1f} KB)")
    print("=" * 60)


if __name__ == "__main__":
    main()
