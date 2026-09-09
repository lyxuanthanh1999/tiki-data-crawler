#!/usr/bin/env bash
# ==============================================================================
# Script dừng khẩn cấp toàn bộ 5 tiến trình Phase 2 Tiki Crawler
# ==============================================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$PROJECT_DIR/logs/retry_crawler.pids"

echo "🛑 Đang kiểm tra và dừng toàn bộ tiến trình Phase 2 Tiki Crawler..."

stopped=0

# 1. Dừng theo PID file nếu tồn tại
if [ -f "$PID_FILE" ]; then
    while read -r pid; do
        if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
            echo "  ⏹️ Dừng process PID: $pid"
            kill -TERM "$pid" >/dev/null 2>&1 || kill -9 "$pid" >/dev/null 2>&1
            stopped=$((stopped + 1))
        fi
    done < "$PID_FILE"
    rm -f "$PID_FILE"
fi

# 2. Quét dọn các tiến trình main.py đang trỏ tới failed_ids_pass2.txt
extra_pids=$(ps aux | grep "[f]ailed_ids_pass2.txt" | awk '{print $2}')
if [ -n "$extra_pids" ]; then
    for pid in $extra_pids; do
        echo "  ⏹️ Dừng tiến trình dư PID: $pid"
        kill -TERM "$pid" >/dev/null 2>&1 || kill -9 "$pid" >/dev/null 2>&1
        stopped=$((stopped + 1))
    done
fi

if [ $stopped -eq 0 ]; then
    echo "ℹ️ Không tìm thấy tiến trình Phase 2 nào đang chạy."
else
    echo "✅ Đã dừng thành công $stopped tiến trình Phase 2!"
fi
