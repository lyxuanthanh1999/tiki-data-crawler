#!/usr/bin/env python3
"""
Dashboard giám sát thời gian thực tiến độ Phase 2 (Quét vét lỗi 94,307 ID).
Theo dõi chi tiết 5 tiến trình, 95 batch và số lượng sản phẩm 'vớt' được.
"""

import json
import os
import subprocess
import time
from pathlib import Path

PARTS_DIR = Path("data/output/retry_pass2/parts")
TOTAL_IDS = 94307
TOTAL_BATCHES = 95
BATCH_RANGES = [
    (1, 1, 19),
    (2, 20, 38),
    (3, 39, 57),
    (4, 58, 76),
    (5, 77, 95),
]


def get_running_processes():
    cmd = ["ps", "-eo", "pid,%cpu,etime,args"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    processes = {}
    for line in res.stdout.strip().split("\n")[1:]:
        parts = line.strip().split(None, 3)
        if len(parts) >= 4 and "main.py" in parts[3] and "retry_pass2" in parts[3]:
            pid = parts[0]
            cpu = parts[1]
            etime = parts[2]
            args = parts[3]
            for p_num, start_b, end_b in BATCH_RANGES:
                if f"--start-batch {start_b}" in args:
                    processes[p_num] = {
                        "pid": pid,
                        "cpu": cpu,
                        "etime": etime,
                    }
                    break
    return processes


def get_process_current_batch(start_b, end_b):
    for b in range(start_b, end_b + 1):
        stat_file = PARTS_DIR / f"products_part_{b:04d}.stats.json"
        if not stat_file.exists():
            json_file = PARTS_DIR / f"products_part_{b:04d}.json"
            last_write = None
            if json_file.exists():
                last_write = time.time() - json_file.stat().st_mtime
            return b, last_write, False
    return end_b, None, True


def calculate_progress():
    total_saved = 0
    total_failed = 0
    resolved_ids = set()

    for b in range(1, TOTAL_BATCHES + 1):
        prog_file = PARTS_DIR / f"products_part_{b:04d}.progress.txt"
        if prog_file.exists():
            with open(prog_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line.isdigit():
                        pid = int(line)
                        if pid not in resolved_ids:
                            resolved_ids.add(pid)
                            total_saved += 1

        fail_file = PARTS_DIR / f"products_part_{b:04d}.failed_permanent.json"
        if fail_file.exists():
            try:
                with open(fail_file, "r", encoding="utf-8") as f:
                    fails = json.load(f)
                    for k in fails:
                        if k.isdigit():
                            pid = int(k)
                            if pid not in resolved_ids:
                                resolved_ids.add(pid)
                                total_failed += 1
            except Exception:
                pass

    total_done = len(resolved_ids)
    remaining = max(0, TOTAL_IDS - total_done)
    pct = (total_done / TOTAL_IDS) * 100 if TOTAL_IDS else 0
    return total_saved, total_failed, total_done, remaining, pct


def main():
    procs = get_running_processes()
    saved, failed, done, remaining, pct = calculate_progress()

    print("=" * 89)
    print("📊 BẢNG TRẠNG THÁI PHASE 2: QUÉT LỖI 94,307 ID (REAL-TIME DASHBOARD)")
    print(f"⏰ Thời gian kiểm tra: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 89)

    for p_num, start_b, end_b in BATCH_RANGES:
        p_info = procs.get(p_num)
        curr_b, last_write, all_done = get_process_current_batch(start_b, end_b)
        range_str = f"Batch {start_b:03d}–{end_b:03d}"
        total_batches_in_proc = end_b - start_b + 1

        if all_done:
            status_str = f"✅ ĐÃ HOÀN TẤT 100% ({total_batches_in_proc}/{total_batches_in_proc} batches)"
            pid_str = "  N/A"
            cpu_str = " 0.0%"
            etime_str = "     --"
        elif p_info:
            pid_str = f"{p_info['pid']:>5}"
            cpu_str = f"{p_info['cpu']:>4}%"
            etime_str = f"{p_info['etime']:>7}"
            write_str = f"(ghi đĩa cách đây {last_write:.1f}s)" if last_write is not None else "(đang khởi tạo)"
            status_str = f"🟢 ĐANG CÀO Batch #{curr_b:04d} {write_str}"
        else:
            pid_str = "  N/A"
            cpu_str = " 0.0%"
            etime_str = "     --"
            done_count = max(0, curr_b - start_b)
            status_str = f"⏹️ Tạm dừng ở Batch #{curr_b:04d} ({done_count}/{total_batches_in_proc} batches xong)"

        print(
            f"Process {p_num} ({range_str}) | PID: {pid_str} | CPU: {cpu_str} | Chạy: {etime_str} | {status_str}"
        )

    print("-" * 89)
    saved_pct = (saved / TOTAL_IDS) * 100 if TOTAL_IDS else 0
    failed_pct = (failed / TOTAL_IDS) * 100 if TOTAL_IDS else 0
    rem_pct = (remaining / TOTAL_IDS) * 100 if TOTAL_IDS else 0

    print(f"📦 Tổng tiến độ Phase 2: {done:,} / {TOTAL_IDS:,} ID ({pct:.2f}%)")
    print(f"   🌟 Sản phẩm sạch 'vớt' được : {saved:,} ({saved_pct:.2f}%)")
    print(f"   ❌ Xác nhận là True 404     : {failed:,} ({failed_pct:.2f}%)")
    print(f"   ⏳ Số ID còn lại cần quét   : {remaining:,} ({rem_pct:.2f}%)")
    print("=" * 89)


if __name__ == "__main__":
    main()
