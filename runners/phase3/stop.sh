#!/usr/bin/env bash
# ==============================================================================
# Script dừng khẩn cấp toàn bộ 5 tiến trình Phase 3
# ==============================================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$PROJECT_DIR/logs/retry_pass3.pids"

echo "🛑 Đang gửi tín hiệu dừng tới các tiến trình Phase 3..."

count=0
if [ -f "$PID_FILE" ]; then
    while read -r pid; do
        if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
            echo "  ⏹️ Dừng PID: $pid"
            kill -TERM "$pid" >/dev/null 2>&1 || true
            count=$((count + 1))
        fi
    done < "$PID_FILE"
    rm -f "$PID_FILE"
fi

# Quét thêm theo pattern nếu còn sót
EXTRA_PIDS=$(pgrep -fl "main.py.*retry_pass3" | awk '{print $1}')
if [ -n "$EXTRA_PIDS" ]; then
    for pid in $EXTRA_PIDS; do
        echo "  ⏹️ Dừng sót PID: $pid"
        kill -TERM "$pid" >/dev/null 2>&1 || true
        count=$((count + 1))
    done
fi

if [ $count -gt 0 ]; then
    echo "✅ Đã dừng an toàn $count tiến trình Phase 3."
else
    echo "ℹ️ Không tìm thấy tiến trình Phase 3 nào đang chạy."
fi
