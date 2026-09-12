#!/usr/bin/env python3
"""
Tổng kết tiến độ & kết quả tất cả phases (Phase 1 → N).
Đọc trực tiếp từ file parts → kết quả real-time, không phụ thuộc stats.json.

Dùng:
  python3 monitor/summary.py
"""

import json
import time
from pathlib import Path
from datetime import datetime

PROJECT_DIR = Path(__file__).resolve().parent.parent
DATA_OUTPUT = PROJECT_DIR / "data" / "output"
INPUT_FILE  = PROJECT_DIR / "data" / "input" / "product_ids.txt"
TOTAL_INPUT_IDS = 200_000


def count_input_ids() -> int:
    if INPUT_FILE.exists():
        with open(INPUT_FILE) as f:
            return sum(1 for l in f if l.strip().isdigit())
    return TOTAL_INPUT_IDS


def get_phase_stats(parts_dir: Path) -> dict:
    rescued    = 0
    failed_404 = 0
    last_write = 0.0

    if not parts_dir.exists():
        return {"rescued": 0, "failed_404": 0, "total": 0, "last_write": None, "exists": False}

    for jf in parts_dir.glob("*.jsonl"):
        try:
            mtime = jf.stat().st_mtime
            if mtime > last_write:
                last_write = mtime
            with open(jf, encoding="utf-8") as f:
                rescued += sum(1 for l in f if l.strip())
        except Exception:
            pass

    for ff in parts_dir.glob("*.failed_permanent.json"):
        try:
            mtime = ff.stat().st_mtime
            if mtime > last_write:
                last_write = mtime
            with open(ff, encoding="utf-8") as f:
                data = json.load(f)
                failed_404 += len(data)
        except Exception:
            pass

    return {
        "rescued": rescued,
        "failed_404": failed_404,
        "total": rescued + failed_404,
        "last_write": last_write if last_write > 0 else None,
        "exists": True,
    }


def discover_phases():
    phases = []
    p1 = DATA_OUTPUT / "concurrency" / "parts"
    if p1.exists():
        phases.append(("Phase 1 (concurrency)", p1))
    if DATA_OUTPUT.exists():
        for d in sorted(DATA_OUTPUT.glob("retry_*/parts")):
            if d.exists():
                idx = len(phases) + 1
                phases.append((f"Phase {idx} ({d.parent.name})", d))
    return phases


def fmt_ago(ts):
    if ts is None:
        return "chưa có"
    ago = time.time() - ts
    if ago < 60:
        return f"{ago:.0f}s trước"
    elif ago < 3600:
        return f"{ago/60:.1f}m trước"
    else:
        return f"{ago/3600:.1f}h trước"


def status_icon(stats, target):
    if not stats["exists"]:
        return "⬜ chưa khởi tạo"
    if stats["total"] == 0:
        return "⬜ chưa có data"
    if target > 0 and stats["total"] >= target:
        return "✅ HOÀN TẤT"
    if stats["last_write"] and (time.time() - stats["last_write"]) < 120:
        return "🟢 ĐANG CHẠY"
    return "🟡 DỪNG/CHỜ"


def main():
    total_input = count_input_ids()
    phases = discover_phases()
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    W = 100
    print("=" * W)
    print(f"{'📊  TỔNG KẾT TIẾN ĐỘ TẤT CẢ PHASES  📊':^{W}}")
    print(f"{'⏰  ' + now:^{W}}")
    print("=" * W)

    all_stats = [(lbl, d, get_phase_stats(d)) for lbl, d in phases]

    # Tính target của từng phase
    targets = []
    prev_failed = total_input
    for _, _, stats in all_stats:
        targets.append(prev_failed)
        if stats["total"] > 0:
            prev_failed = stats["failed_404"]

    # Header bảng
    print(f"  {'Phase':<30} {'Vớt được':>10} {'True 404':>10} {'Tổng XL':>9} {'/ Target':>9}  {'%':>6}  Status")
    print("-" * W)

    for i, (label, parts_dir, stats) in enumerate(all_stats):
        target = targets[i]
        icon   = status_icon(stats, target)
        pct    = stats["total"] / target * 100 if target > 0 else 0.0
        ago    = fmt_ago(stats["last_write"])
        print(
            f"  {label:<30} "
            f"{stats['rescued']:>10,} "
            f"{stats['failed_404']:>10,} "
            f"{stats['total']:>9,} "
            f"/{target:>8,} "
            f"{pct:>6.2f}%  "
            f"{icon}  ({ago})"
        )

    print("=" * W)

    # Tổng kết
    total_rescued_all = sum(s["rescued"] for _, _, s in all_stats)
    last_stats  = all_stats[-1][2] if all_stats else {}
    last_target = targets[-1]      if targets   else total_input
    last_remaining = last_target - last_stats.get("total", 0)
    last_pct_done  = last_stats.get("total", 0) / last_target * 100 if last_target > 0 else 0

    print(f"  🌟 Tổng sản phẩm VỚT được (tổng tất cả phases) : {total_rescued_all:,}")
    print(f"  ❌ ID còn lại chưa xử lý (phase mới nhất)      : {last_remaining:,}")
    print(f"  📦 Tiến độ phase mới nhất                       : {last_stats.get('total',0):,} / {last_target:,}  ({last_pct_done:.2f}%)")
    print("=" * W)

    # Flow diagram
    print()
    print(f"  📈 Luồng kết quả từ {total_input:,} ID gốc:")
    for i, (label, _, stats) in enumerate(all_stats):
        arrow = "  " if i == 0 else "  └→"
        print(f"  {arrow} {label:<28}: vớt {stats['rescued']:,}  →  còn lỗi {stats['failed_404']:,}")
    print()


if __name__ == "__main__":
    main()
