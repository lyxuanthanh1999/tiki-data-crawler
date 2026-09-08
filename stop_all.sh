#!/bin/bash
# ==============================================================================
# Tiki Crawler - Stop All Processes
# Dừng an toàn toàn bộ 5 tiến trình cào dữ liệu
# ==============================================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOGS_DIR="$PROJECT_DIR/logs"
PID_FILE="$LOGS_DIR/crawler.pids"

echo "🛑 Đang kiểm tra và dừng toàn bộ tiến trình Tiki Crawler..."

# 1. Dừng theo file PID nếu có
if [ -f "$PID_FILE" ]; then
    while read -r pid; do
        if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
            echo "  ⏹️ Dừng process PID: $pid"
            kill -TERM "$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null
        fi
    done < "$PID_FILE"
    rm -f "$PID_FILE"
fi

# 2. Dừng dự phòng theo tên lệnh (main.py với start-batch)
pkill -f "main.py.*--start-batch" 2>/dev/null

sleep 1

# 3. Kiểm tra xem còn tiến trình nào không
REMAINING=$(pgrep -f "main.py.*--start-batch" | wc -l | tr -d ' ')
if [ "$REMAINING" -eq 0 ]; then
    echo "✅ Tất cả tiến trình Tiki Crawler đã được dừng an toàn!"
else
    echo "⚠️ Còn $REMAINING tiến trình đang chạy. Đang ép buộc dừng (kill -9)..."
    pkill -9 -f "main.py.*--start-batch" 2>/dev/null
    echo "✅ Đã ép dừng hoàn toàn."
fi
