#!/bin/bash
# ==============================================================================
# Tiki Crawler Phase 4 Launcher (.command)
# Double-click trong Finder hoặc chạy từ Terminal
# Dùng caffeinate để ngăn macOS ngủ + auto-restart nếu mất mạng
# ==============================================================================

PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$PROJECT_DIR" || exit 1

echo "======================================================================"
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] Khởi động Phase 4: Vớt 75,701 ID lỗi"
echo "☕ Kích hoạt Caffeinate (ngăn macOS sleep)..."
echo "======================================================================"

while true; do
    caffeinate -dimsu ./runners/phase4/run.sh
    EXIT_CODE=$?

    if [ $EXIT_CODE -eq 0 ]; then
        echo "======================================================================"
        echo "🎉 [$(date '+%Y-%m-%d %H:%M:%S')] PHASE 4 HOÀN TẤT 100%!"
        echo "   Chạy: python3 tools/merge_parts.py để tổng hợp dữ liệu."
        echo "======================================================================"
        break
    elif [ $EXIT_CODE -eq 130 ]; then
        echo "🛑 Người dùng dừng (Ctrl+C). Thoát."
        break
    else
        echo "⚠️  Lỗi code $EXIT_CODE (mất mạng hoặc crash). Thử lại sau 10 giây..."
        sleep 10
    fi
done
