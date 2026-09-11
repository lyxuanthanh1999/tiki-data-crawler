#!/usr/bin/env bash
# Dừng toàn bộ tiến trình của một run cụ thể.
# Dùng: ./runners/stop_retry.sh --name <run_name>

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUN_NAME=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --name) RUN_NAME="$2"; shift 2 ;;
        *) echo "Unknown: $1"; exit 1 ;;
    esac
done

if [[ -z "$RUN_NAME" ]]; then
    # Nếu không có --name, liệt kê tất cả PID files đang có
    echo "Available runs:"
    ls "$PROJECT_DIR/logs/"*.pids 2>/dev/null | sed 's/.*retry_//;s/\.pids//' || echo "(none)"
    echo ""
    echo "Usage: ./runners/stop_retry.sh --name <run_name>"
    exit 0
fi

PID_FILE="$PROJECT_DIR/logs/retry_${RUN_NAME}.pids"

if [[ ! -f "$PID_FILE" ]]; then
    echo "⚠️  Không tìm thấy PID file: $PID_FILE"
    echo "   Có thể run '$RUN_NAME' đã hoàn tất hoặc chưa chạy."
    exit 0
fi

echo "🛑 Đang dừng run: $RUN_NAME ..."
KILLED=0
while read -r pid; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null
        echo "   ✅ Đã gửi SIGTERM → PID $pid"
        KILLED=$((KILLED + 1))
    fi
done < "$PID_FILE"

rm -f "$PID_FILE"
echo "🛑 Đã dừng $KILLED tiến trình của run '$RUN_NAME'."
