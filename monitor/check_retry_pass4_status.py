#!/usr/bin/env python3
"""
Dashboard giám sát thời gian thực tiến độ Phase 4 (Vớt 75,701 ID lỗi).
Theo dõi chi tiết 5 tiến trình, 76 batch.
"""

import json
import os
import subprocess
import time
from pathlib import Path

PARTS_DIR = Path("data/output/retry_pass4/parts")
TOTAL_IDS = 75701
TOTAL_BATCHES = 76
BATCH_RANGES = [
    (1,  1, 16),   # Process 1
    (2, 17, 32),   # Process 2
    (3, 33, 48),   # Process 3
    (4, 49, 64),   # Process 4
    (5, 65, 76),   # Process 5
]
PID_FILE = Path("logs/retry_pass4.pids")
LOG_PREFIX = "logs/retry_p4"


def get_pids():
    if not PID_FILE.exists():
        return {}
    pids = {}
    lines = [l.strip() for l in PID_FILE.read_text().splitlines() if l.strip().isdigit()]
    for i, pid in enumerate(lines, 1):
        pids[i] = int(pid)
    return pids


def get_process_info(pid):
    try:
        r = subprocess.run(["ps", "-p", str(pid), "-o", "pid,%cpu,etime"],
                           capture_output=True, text=True)
        parts = r.stdout.strip().split("\n")
        if len(parts) >= 2:
            cols = parts[1].split()
            return {"alive": True, "cpu": float(cols[1]), "etime": cols[2] if len(cols) > 2 else "--"}
    except Exception:
        pass
    return {"alive": False, "cpu": 0.0, "etime": "--"}


def get_batch_stats(start_b, end_b):
    rescued, fails, last_write = 0, 0, None
    for b in range(start_b, end_b + 1):
        sf = PARTS_DIR / f"products_part_{b:04d}.stats.json"
        pf = PARTS_DIR / f"products_part_{b:04d}.progress.txt"
        jf = PARTS_DIR / f"products_part_{b:04d}.jsonl"
        ff = PARTS_DIR / f"products_part_{b:04d}.failed_permanent.json"

        # Đếm sản phẩm vớt được (saved/rescued)
        if pf.exists():
            try:
                with open(pf, "r", encoding="utf-8") as f:
                    rescued += sum(1 for line in f if line.strip().isdigit())
            except Exception:
                pass
            mtime = pf.stat().st_mtime
            if last_write is None or mtime > last_write:
                last_write = mtime
        elif jf.exists():
            try:
                with open(jf, "r", encoding="utf-8") as f:
                    rescued += sum(1 for line in f if line.strip())
            except Exception:
                pass
            mtime = jf.stat().st_mtime
            if last_write is None or mtime > last_write:
                last_write = mtime

        # Đếm sản phẩm xác nhận 404
        if ff.exists():
            try:
                with open(ff, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    fails += len(data)
            except Exception:
                pass
            mtime = ff.stat().st_mtime
            if last_write is None or mtime > last_write:
                last_write = mtime

        if sf.exists():
            mtime = sf.stat().st_mtime
            if last_write is None or mtime > last_write:
                last_write = mtime

    completed = sum(1 for b in range(start_b, end_b + 1)
                    if (PARTS_DIR / f"products_part_{b:04d}.stats.json").exists())
    return {"total": rescued + fails, "rescued": rescued, "fails": fails,
            "completed": completed, "total_b": end_b - start_b + 1, "last_write": last_write}


def main():
    print("=" * 90)
    print(f"📊 BẢNG TRẠNG THÁI PHASE 4: VỚT LỖI {TOTAL_IDS:,} ID (REAL-TIME DASHBOARD)")
    print(f"⏰ Thời gian kiểm tra: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90)

    pids = get_pids()
    total_rescued, total_fails, total_processed = 0, 0, 0

    for proc_idx, start_b, end_b in BATCH_RANGES:
        pid = pids.get(proc_idx, 0)
        stats = get_batch_stats(start_b, end_b)
        proc = get_process_info(pid) if pid else {"alive": False, "cpu": 0.0, "etime": "--"}

        total_rescued += stats["rescued"]
        total_fails += stats["fails"]
        total_processed += stats["rescued"] + stats["fails"]

        if proc["alive"]:
            cur_b = start_b
            for b in range(start_b, end_b + 1):
                if not (PARTS_DIR / f"products_part_{b:04d}.stats.json").exists():
                    cur_b = b; break
                cur_b = b + 1
            write_tag = f"{time.time() - stats['last_write']:.1f}s" if stats["last_write"] else "?"
            status = f"🟢 ĐANG CÀO Batch #{cur_b:04d} (ghi đĩa cách đây {write_tag})"
        elif stats["completed"] == stats["total_b"]:
            status = f"✅ ĐÃ HOÀN TẤT 100% ({stats['completed']}/{stats['total_b']} batches)"
        else:
            status = f"⏸️  ĐÃ DỪNG ({stats['completed']}/{stats['total_b']} batches)"

        print(
            f"Process {proc_idx} (Batch {start_b:03d}–{end_b:03d}) | "
            f"PID: {pid if pid else 'N/A':>6} | CPU: {proc['cpu']:>4.1f}% | "
            f"Chạy: {proc['etime']:>8} | {status}"
        )

    remaining = TOTAL_IDS - total_processed
    pct = total_processed / TOTAL_IDS * 100 if TOTAL_IDS else 0

    print("-" * 90)
    print(f"📦 Tổng tiến độ Phase 4: {total_processed:,} / {TOTAL_IDS:,} ID ({pct:.2f}%)")
    print(f"   🌟 Sản phẩm sạch 'vớt' được : {total_rescued:,} ({total_rescued/TOTAL_IDS*100:.2f}%)")
    print(f"   ❌ Xác nhận là True 404     : {total_fails:,} ({total_fails/TOTAL_IDS*100:.2f}%)")
    print(f"   ⏳ Số ID còn lại cần quét   : {remaining:,} ({100-pct:.2f}%)")
    print("=" * 90)


if __name__ == "__main__":
    main()
