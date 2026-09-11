#!/usr/bin/env bash
# ==============================================================================
# Tiki Product Crawler - Phase 4: Deep Retry 75,701 Failed IDs
# Điều phối 5 tiến trình chạy ngầm qua 5 Cloudflare Proxies.
# Áp dụng Sweet Spot Pacing: Concurrency 6, Delay 1.2s-2.2s (~6.2 req/s)
# ==============================================================================

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$PROJECT_DIR"

LOGS_DIR="$PROJECT_DIR/logs"
OUTPUT_DIR="$PROJECT_DIR/data/output/retry_pass4/parts"
INPUT_FILE="$PROJECT_DIR/data/input/failed_ids_pass4.txt"
PID_FILE="$LOGS_DIR/retry_pass4.pids"
TOTAL_BATCHES=76   # ceil(75701 / 1000)

# Python bin
if [ -f "$PROJECT_DIR/venv/bin/python" ]; then
    PYTHON_BIN="$PROJECT_DIR/venv/bin/python"
else
    PYTHON_BIN="$(which python3)"
fi

mkdir -p "$LOGS_DIR" "$OUTPUT_DIR" "$(dirname "$INPUT_FILE")"

echo "======================================================================"
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] Khởi động Phase 4: Vớt 75,701 ID lỗi (5 Processes)"
echo "📂 Project Dir: $PROJECT_DIR"
echo "📄 Input File : $INPUT_FILE"
echo "📁 Output Dir : $OUTPUT_DIR"
echo "🐍 Python Bin : $PYTHON_BIN"
echo "⚡ Cấu hình   : Concurrency 6, Delay 1.2-2.2s (Sweet Spot Pacing ~6.2 req/s)"
echo "======================================================================"

# Bước 1: Tạo file input từ failed_permanent.json nếu chưa có
if [ ! -f "$INPUT_FILE" ]; then
    echo "🔄 Đang tạo $INPUT_FILE từ products_output.failed_permanent.json ..."
    "$PYTHON_BIN" -c "
import json
data = json.loads(open('data/output/concurrency/products_output.failed_permanent.json').read())
ids = sorted(int(k) for k in data.keys())
with open('$INPUT_FILE', 'w') as f:
    f.write('\n'.join(str(i) for i in ids) + '\n')
print(f'✅ Extracted {len(ids):,} IDs -> $INPUT_FILE')
"
fi

TOTAL_IDS=$(wc -l < "$INPUT_FILE" | tr -d ' ')
echo "📊 Total IDs: $TOTAL_IDS | Total Batches: $TOTAL_BATCHES"

# Bước 2: Kiểm tra mạng
echo "🌐 Đang kiểm tra kết nối mạng..."
MAX_WAIT_NET=60; WAITED=0
while ! ping -c 1 1.1.1.1 >/dev/null 2>&1 && ! ping -c 1 8.8.8.8 >/dev/null 2>&1; do
    sleep 3; WAITED=$((WAITED + 3))
    if [ $WAITED -ge $MAX_WAIT_NET ]; then
        echo "❌ Không có kết nối mạng sau ${MAX_WAIT_NET}s." && exit 1
    fi
done
echo "✅ Kết nối Internet sẵn sàng!"

# Bước 3: Dọn PID cũ
if [ -f "$PID_FILE" ]; then
    while read -r pid; do
        [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
fi

COMMON_ARGS="--input $INPUT_FILE --output-dir $OUTPUT_DIR --batch-size 1000 --concurrency 6 --delay-min 1.2 --delay-max 2.2 --auto-wait --max-waf-retries 0 --auto-wait-interval 60 --no-seed-existing"

echo "🚀 Đang khởi động 5 tiến trình Phase 4 song song..."

# Worker 1: Batch 001-016
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 1 --end-batch 16 \
    --worker-url https://tiki-proxy-worker-1.lyxuanthanh1999.workers.dev \
    >> "$LOGS_DIR/retry_p4_1.log" 2>&1 &
P1=$!; echo "$P1" >> "$PID_FILE"
echo "  🔹 [Process 1] PID: $P1 | Batches: 0001-0016 | Worker 1 -> logs/retry_p4_1.log"

# Worker 2: Batch 017-032
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 17 --end-batch 32 \
    --worker-url https://tiki-proxy-worker-2.lyxuanthanh1999.workers.dev \
    >> "$LOGS_DIR/retry_p4_2.log" 2>&1 &
P2=$!; echo "$P2" >> "$PID_FILE"
echo "  🔹 [Process 2] PID: $P2 | Batches: 0017-0032 | Worker 2 -> logs/retry_p4_2.log"

# Worker 3: Batch 033-048
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 33 --end-batch 48 \
    --worker-url https://tiki-proxy-worker-3.lyxuanthanh1999.workers.dev \
    >> "$LOGS_DIR/retry_p4_3.log" 2>&1 &
P3=$!; echo "$P3" >> "$PID_FILE"
echo "  🔹 [Process 3] PID: $P3 | Batches: 0033-0048 | Worker 3 -> logs/retry_p4_3.log"

# Worker 4: Batch 049-064
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 49 --end-batch 64 \
    --worker-url https://tiki-proxy-worker-4.lyxuanthanh1999.workers.dev \
    >> "$LOGS_DIR/retry_p4_4.log" 2>&1 &
P4=$!; echo "$P4" >> "$PID_FILE"
echo "  🔹 [Process 4] PID: $P4 | Batches: 0049-0064 | Worker 4 -> logs/retry_p4_4.log"

# Worker 5: Batch 065-076
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 65 --end-batch 76 \
    --worker-url https://tiki-proxy-worker-5.lyxuanthanh1999.workers.dev \
    >> "$LOGS_DIR/retry_p4_5.log" 2>&1 &
P5=$!; echo "$P5" >> "$PID_FILE"
echo "  🔹 [Process 5] PID: $P5 | Batches: 0065-0076 | Worker 5 -> logs/retry_p4_5.log"

echo "======================================================================"
echo "✅ Cả 5 tiến trình Phase 4 đã chạy ngầm thành công!"
echo "📊 Theo dõi: python3 monitor/check_retry_pass4_status.py"
echo "📄 Live log: tail -f logs/retry_p4_*.log"
echo "🛑 Dừng   : ./runners/phase4/stop.sh"
echo "======================================================================"

wait "$P1"; E1=$?
wait "$P2"; E2=$?
wait "$P3"; E3=$?
wait "$P4"; E4=$?
wait "$P5"; E5=$?
rm -f "$PID_FILE"

echo "======================================================================"
echo "🎉 [$(date '+%Y-%m-%d %H:%M:%S')] TẤT CẢ 5 TIẾN TRÌNH PHASE 4 ĐÃ HOÀN TẤT!"
echo "   Status: P1=$E1, P2=$E2, P3=$E3, P4=$E4, P5=$E5"
echo "======================================================================"

[ $E1 -eq 0 ] && [ $E2 -eq 0 ] && [ $E3 -eq 0 ] && [ $E4 -eq 0 ] && [ $E5 -eq 0 ] && exit 0 || exit 1
