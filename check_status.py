#!/usr/bin/env python3
"""
Tiki Crawler - Live Multi-Process Dashboard
Kiểm tra trạng thái thời gian thực của 5 tiến trình đang cào song song.
"""

import glob
import json
import os
import re
import subprocess
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).parent
PARTS_DIR = PROJECT_DIR / "data/output/concurrency/parts"

PROCESS_CONFIGS = [
    {"p": 1, "batches": (1, 40), "worker": "Worker 1 (tyanh185)", "log": "logs/p1.log"},
    {"p": 2, "batches": (41, 80), "worker": "Worker 2 (tyanh185)", "log": "logs/p2.log"},
    {"p": 3, "batches": (81, 120), "worker": "Worker 3 (tyanh185)", "log": "logs/p3.log"},
    {"p": 4, "batches": (121, 160), "worker": "Worker 4 (tyanh185)", "log": "logs/p4.log"},
    {"p": 5, "batches": (161, 200), "worker": "Worker 5 (tyanh185)", "log": "logs/p5.log"},
]


def get_running_processes():
    """Lấy danh sách các tiến trình main.py đang chạy từ hệ điều hành."""
    try:
        output = subprocess.check_output(
            ["ps", "-eo", "pid,%cpu,etime,command"],
            text=True,
        )
    except Exception:
        return {}

    procs = {}
    for line in output.splitlines():
        if "main.py" in line and "--start-batch" in line:
            parts = line.strip().split(None, 3)
            if len(parts) >= 4:
                pid, cpu, etime, cmd = parts[0], parts[1], parts[2], parts[3]
                m_start = re.search(r"--start-batch\s+(\d+)", cmd)
                m_end = re.search(r"--end-batch\s+(\d+)", cmd)
                if m_start and m_end:
                    start_b = int(m_start.group(1))
                    procs[start_b] = {"pid": pid, "cpu": cpu, "etime": etime}
    return procs


def main():
    running_procs = get_running_processes()

    print("=========================================================================================")
    print("📊 BẢNG TRẠNG THÁI 5 TIẾN TRÌNH CÀO DỮ LIỆU TIKI (REAL-TIME DASHBOARD)")
    print(f"⏰ Thời gian kiểm tra: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=========================================================================================")

    all_batch_ids = set()
    for f in PARTS_DIR.glob("products_part_*.*"):
        m = re.search(r"products_part_(\d+)\.", f.name)
        if m:
            all_batch_ids.add(int(m.group(1)))

    for cfg in PROCESS_CONFIGS:
        p_num = cfg["p"]
        start_b, end_b = cfg["batches"]
        proc_info = running_procs.get(start_b)

        # Kiểm tra batch đang xử lý gần nhất trong log hoặc trên đĩa
        latest_batch = start_b
        latest_write_sec = None
        for b in range(start_b, end_b + 1):
            jsonl = PARTS_DIR / f"products_part_{b:04d}.jsonl"
            json_f = PARTS_DIR / f"products_part_{b:04d}.json"
            if jsonl.exists() or json_f.exists():
                latest_batch = b
                if jsonl.exists():
                    latest_write_sec = time.time() - jsonl.stat().st_mtime

        # Kiểm tra số batch đã xong trong dải này
        done_in_range = sum(1 for b in range(start_b, end_b + 1) if b in all_batch_ids)

        if proc_info:
            pid = proc_info["pid"]
            cpu = proc_info["cpu"]
            etime = proc_info["etime"]
            write_status = f"(ghi đĩa cách đây {latest_write_sec:.1f}s)" if latest_write_sec is not None else ""
            status_text = f"🟢 ĐANG CÀO Batch #{latest_batch:04d} {write_status}"
            print(f"Process {p_num} (Batch {start_b:03d}–{end_b:03d}) | PID: {pid:>5} | CPU: {cpu:>4}% | Chạy: {etime:>8} | {status_text}")
        else:
            if done_in_range >= (end_b - start_b + 1):
                status_text = f"✅ ĐÃ HOÀN TẤT 100% ({done_in_range}/{end_b - start_b + 1} batches)"
            else:
                status_text = f"⏹️ Tạm dừng ở Batch #{latest_batch:04d} ({done_in_range}/{end_b - start_b + 1} batches xong)"
            print(f"Process {p_num} (Batch {start_b:03d}–{end_b:03d}) | PID:   N/A | CPU:  0.0% | Chạy:      -- | {status_text}")

    print("-----------------------------------------------------------------------------------------")

    # Thống kê nhanh tổng quan
    unique_prods = set()
    unique_failed = set()
    for b in all_batch_ids:
        jsonl = PARTS_DIR / f"products_part_{b:04d}.jsonl"
        json_f = PARTS_DIR / f"products_part_{b:04d}.json"
        perm_f = PARTS_DIR / f"products_part_{b:04d}.failed_permanent.json"

        if jsonl.exists():
            for line in open(jsonl):
                if line.strip():
                    try:
                        unique_prods.add(json.loads(line)["id"])
                    except Exception:
                        pass
        elif json_f.exists():
            try:
                for item in json.load(open(json_f)):
                    unique_prods.add(item["id"])
            except Exception:
                pass

        if perm_f.exists():
            try:
                unique_failed.update(json.load(open(perm_f)).keys())
            except Exception:
                pass

    total_done = len(unique_prods) + len(unique_failed)
    total_target = 200000
    pct = (total_done / total_target) * 100

    print(f"📦 Tổng tiến độ toàn dự án: {total_done:,} / {total_target:,} ID ({pct:.2f}%)")
    print(f"   ✅ Sản phẩm sạch thành công : {len(unique_prods):,} ({(len(unique_prods)/total_target)*100:.2f}%)")
    print(f"   ❌ Lỗi vĩnh viễn (404/xóa)  : {len(unique_failed):,} ({(len(unique_failed)/total_target)*100:.2f}%)")
    print(f"   ⏳ Số ID còn lại cần cào    : {total_target - total_done:,} ({(100-pct):.2f}%)")
    print("=========================================================================================")


if __name__ == "__main__":
    main()
