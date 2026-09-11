#!/usr/bin/env python3
"""
Generic Real-time Dashboard cho bất kỳ retry run nào.
Dùng: python3 monitor/check_retry.py --name <run_name>
      python3 monitor/check_retry.py           (liệt kê tất cả runs)
"""

import argparse
import json
import os
import subprocess
import time
from pathlib import Path

PROJECT_DIR = Path(__file__).parent.parent


def list_runs():
    """Liệt kê tất cả các run có config file."""
    configs = sorted(PROJECT_DIR.glob("logs/retry_*.config.json"))
    if not configs:
        print("⚠️  Chưa có run nào. Chạy: ./runners/retry.sh --input <file>")
        return
    print("📋 Danh sách các retry runs:")
    for cfg_path in configs:
        try:
            cfg = json.loads(cfg_path.read_text())
            name = cfg.get("run_name", "?")
            total = cfg.get("total_ids", 0)
            started = cfg.get("started_at", "?")
            print(f"  🔹 {name:<40} | {total:>8,} IDs | started: {started}")
        except Exception:
            print(f"  ⚠️  {cfg_path.name} (lỗi đọc config)")
    print("")
    print("Xem chi tiết: python3 monitor/check_retry.py --name <run_name>")


def get_process_info(pid: int) -> dict:
    """Lấy thông tin CPU/runtime của process."""
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "pid,%cpu,etime"],
            capture_output=True, text=True
        )
        lines = result.stdout.strip().split("\n")
        if len(lines) >= 2:
            parts = lines[1].split()
            cpu = float(parts[1]) if len(parts) > 1 else 0.0
            etime = parts[2] if len(parts) > 2 else "--"
            return {"alive": True, "cpu": cpu, "etime": etime}
    except Exception:
        pass
    return {"alive": False, "cpu": 0.0, "etime": "--"}


def get_batch_stats(parts_dir: Path, start_b: int, end_b: int) -> dict:
    """Thống kê tiến độ của một range batch."""
    total_ids = 0
    rescued = 0
    confirmed_404 = 0
    last_write = None

    for b in range(start_b, end_b + 1):
        stat_file = parts_dir / f"products_part_{b:04d}.stats.json"
        prog_file = parts_dir / f"products_part_{b:04d}.progress.txt"
        fail_file = parts_dir / f"products_part_{b:04d}.failed_permanent.json"
        jsonl_file = parts_dir / f"products_part_{b:04d}.jsonl"

        if stat_file.exists():
            try:
                s = json.loads(stat_file.read_text())
                total_ids += s.get("total", 0)
                rescued += s.get("saved", 0)
                confirmed_404 += s.get("failed_permanent", 0)
            except Exception:
                pass
        elif prog_file.exists():
            total_ids += sum(1 for _ in prog_file.read_text().splitlines() if _.strip())
        
        # Track thời gian ghi file gần nhất
        for f in [jsonl_file, stat_file]:
            if f.exists():
                mtime = f.stat().st_mtime
                if last_write is None or mtime > last_write:
                    last_write = mtime

    completed_batches = sum(
        1 for b in range(start_b, end_b + 1)
        if (parts_dir / f"products_part_{b:04d}.stats.json").exists()
    )

    return {
        "total_ids": total_ids,
        "rescued": rescued,
        "confirmed_404": confirmed_404,
        "completed_batches": completed_batches,
        "total_batches": end_b - start_b + 1,
        "last_write": last_write,
    }


def show_dashboard(run_name: str, watch: bool = False):
    """Hiển thị dashboard cho một run."""
    cfg_path = PROJECT_DIR / "logs" / f"retry_{run_name}.config.json"
    if not cfg_path.exists():
        print(f"❌ Không tìm thấy config: {cfg_path}")
        print(f"   Chạy: ./runners/retry.sh --input <file> --name {run_name}")
        return

    cfg = json.loads(cfg_path.read_text())
    parts_dir = Path(cfg["output_dir"])
    total_ids = cfg["total_ids"]
    total_batches = cfg["total_batches"]
    num_workers = cfg["num_workers"]
    batches_per_worker = cfg["batches_per_worker"]
    pid_file = Path(cfg["pid_file"])
    log_prefix = cfg["log_prefix"]

    # Đọc PIDs
    pids = []
    if pid_file.exists():
        pids = [int(p.strip()) for p in pid_file.read_text().splitlines() if p.strip().isdigit()]
    
    # Pad PIDs nếu thiếu (khi process đã xong)
    while len(pids) < num_workers:
        pids.append(0)

    while True:
        print("\033[H\033[J" if watch else "", end="")  # Clear screen if watch mode
        print("=" * 90)
        print(f"📊 DASHBOARD: {run_name.upper()} | {total_ids:,} IDs | {total_batches} batches")
        print(f"⏰ {time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 90)

        total_rescued = 0
        total_confirmed_404 = 0
        total_processed = 0

        for i in range(num_workers):
            start_b = i * batches_per_worker + 1
            end_b = min((i + 1) * batches_per_worker, total_batches)
            if start_b > total_batches:
                break

            pid = pids[i] if i < len(pids) else 0
            stats = get_batch_stats(parts_dir, start_b, end_b)
            proc = get_process_info(pid) if pid > 0 else {"alive": False, "cpu": 0.0, "etime": "--"}

            total_rescued += stats["rescued"]
            total_confirmed_404 += stats["confirmed_404"]
            total_processed += stats["rescued"] + stats["confirmed_404"]

            # Status badge
            if proc["alive"]:
                # Tìm batch đang chạy (batch cuối chưa có stats)
                current_batch = start_b
                for b in range(start_b, end_b + 1):
                    sf = parts_dir / f"products_part_{b:04d}.stats.json"
                    if not sf.exists():
                        current_batch = b
                        break
                    current_batch = b + 1

                # Thời gian ghi file gần nhất
                if stats["last_write"]:
                    delta = time.time() - stats["last_write"]
                    write_tag = f"{delta:.0f}s trước"
                else:
                    write_tag = "chưa ghi"
                status = f"🟢 ĐANG CÀO | Batch #{current_batch:04d} (ghi {write_tag})"
            elif stats["completed_batches"] == stats["total_batches"]:
                status = f"✅ HOÀN TẤT 100% ({stats['completed_batches']}/{stats['total_batches']} batches)"
            else:
                status = f"⏸️  ĐÃ DỪNG ({stats['completed_batches']}/{stats['total_batches']} batches)"

            cpu_tag = f"{proc['cpu']:.1f}%" if proc["alive"] else " 0.0%"
            print(
                f"Worker {i+1} (Batch {start_b:03d}–{end_b:03d}) | "
                f"PID: {pid if pid else 'N/A':>6} | CPU: {cpu_tag:>5} | "
                f"Chạy: {proc['etime']:>8} | {status}"
            )

        total_remaining = total_ids - total_processed
        pct = total_processed / total_ids * 100 if total_ids else 0
        rescued_pct = total_rescued / total_ids * 100 if total_ids else 0
        fail_pct = total_confirmed_404 / total_ids * 100 if total_ids else 0

        print("-" * 90)
        print(f"📦 Tiến độ: {total_processed:,} / {total_ids:,} IDs ({pct:.2f}%)")
        print(f"   🌟 Sản phẩm vớt được : {total_rescued:,} ({rescued_pct:.2f}%)")
        print(f"   ❌ Xác nhận True 404 : {total_confirmed_404:,} ({fail_pct:.2f}%)")
        print(f"   ⏳ Còn lại           : {total_remaining:,} ({100-pct:.2f}%)")
        print("=" * 90)

        if not watch:
            break
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description="Generic Retry Crawler Dashboard")
    parser.add_argument("--name", type=str, default="", help="Tên run (từ --name khi chạy retry.sh)")
    parser.add_argument("--watch", "-w", action="store_true", help="Auto-refresh mỗi 5 giây")
    args = parser.parse_args()

    if not args.name:
        list_runs()
    else:
        show_dashboard(args.name, watch=args.watch)


if __name__ == "__main__":
    main()
