#!/usr/bin/env bash
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PID_FILE="$PROJECT_DIR/logs/retry_pass4.pids"
if [ ! -f "$PID_FILE" ]; then echo "⚠️  Không tìm thấy PID file — Phase 4 chưa chạy hoặc đã hoàn tất."; exit 0; fi
echo "🛑 Đang dừng Phase 4..."
KILLED=0
while read -r pid; do
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null && echo "   ✅ Đã dừng PID $pid" && KILLED=$((KILLED+1))
    fi
done < "$PID_FILE"
rm -f "$PID_FILE"
echo "🛑 Đã dừng $KILLED tiến trình Phase 4."
