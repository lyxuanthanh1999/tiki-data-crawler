#!/usr/bin/env python3
"""
Trích xuất danh sách các ID bị lỗi vĩnh viễn (404) từ Phase 1 ra file input cho Phase 2.
"""

import json
from pathlib import Path
import sys

def main():
    failed_json = Path("data/output/concurrency/products_output.failed_permanent.json")
    output_txt = Path("data/input/failed_ids_pass2.txt")
    
    if not failed_json.exists():
        print(f"❌ Không tìm thấy file: {failed_json}")
        sys.exit(1)
        
    print(f"📂 Đang đọc danh sách lỗi từ: {failed_json}...")
    with open(failed_json, "r", encoding="utf-8") as f:
        failed_data = json.load(f)
        
    ids = sorted([int(k) for k in failed_data.keys()])
    print(f"✅ Đã tìm thấy {len(ids):,} ID lỗi.")
    
    output_txt.parent.mkdir(parents=True, exist_ok=True)
    with open(output_txt, "w", encoding="utf-8") as f:
        for pid in ids:
            f.write(f"{pid}\n")
            
    print(f"🎉 Đã lưu danh sách vào: {output_txt}")
    print(f"📊 Tổng số ID: {len(ids):,}")

if __name__ == "__main__":
    main()
