#!/usr/bin/env bash
# ==============================================================================
# Tiki Product Crawler - Phase 3: Deep Dead Letter Queue (DLQ) Re-crawl
# Điều phối 5 tiến trình chạy ngầm qua 5 Cloudflare Proxies để quét vét 83,804 ID lỗi.
# Áp dụng công thức Sweet Spot Pacing: Concurrency 6, Delay 1.2s-2.2s (~6.2 req/s)
# ==============================================================================

set -e

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR"

LOGS_DIR="$PROJECT_DIR/logs"
OUTPUT_DIR="$PROJECT_DIR/data/output/retry_pass3/parts"
INPUT_FILE="$PROJECT_DIR/data/input/failed_ids_pass3.txt"
PID_FILE="$LOGS_DIR/retry_pass3.pids"

mkdir -p "$LOGS_DIR"
mkdir -p "$OUTPUT_DIR"

# 1. Kiểm tra môi trường Python
if [ -f "$PROJECT_DIR/venv/bin/python" ]; then
    PYTHON_BIN="$PROJECT_DIR/venv/bin/python"
elif [ -f "$PROJECT_DIR/venv/bin/python3" ]; then
    PYTHON_BIN="$PROJECT_DIR/venv/bin/python3"
else
    PYTHON_BIN="python3"
fi

echo "======================================================================"
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] Khởi động Phase 3: Quét lỗi 83,804 ID (5 Processes)"
echo "📂 Project Dir: $PROJECT_DIR"
echo "📄 Input File : $INPUT_FILE"
echo "📁 Output Dir : $OUTPUT_DIR"
echo "🐍 Python Bin : $PYTHON_BIN"
echo "⚡ Cấu hình   : Concurrency 6, Delay 1.2-2.2s (Sweet Spot Pacing ~6.2 req/s)"
echo "======================================================================"

# Kiểm tra file input
if [ ! -f "$INPUT_FILE" ]; then
    echo "❌ File $INPUT_FILE không tồn tại! Hãy chạy python3 scripts/extract_failed_ids_pass3.py trước."
    exit 1
fi

# 2. Kiểm tra kết nối Internet
check_internet() {
    curl -s --head --request GET "https://api.tiki.vn" --max-time 5 > /dev/null 2>&1
    return $?
}

echo "🌐 Đang kiểm tra kết nối mạng..."
while ! check_internet; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') [WAIT] Không có kết nối mạng / Tiki không phản hồi. Đang chờ kết nối lại (thử lại sau 15s)..."
    sleep 15
done
echo "✅ Kết nối Internet sẵn sàng!"

# Xóa PID file cũ nếu có
rm -f "$PID_FILE"
touch "$PID_FILE"

# Dọn dẹp tiến trình con khi nhận tín hiệu dừng
cleanup() {
    echo ""
    echo "⚠️ Nhận tín hiệu dừng! Đang gửi tín hiệu kết thúc an toàn đến các tiến trình con..."
    if [ -f "$PID_FILE" ]; then
        while read -r pid; do
            if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
                kill -TERM "$pid" >/dev/null 2>&1
            fi
        done < "$PID_FILE"
        rm -f "$PID_FILE"
    fi
    echo "🛑 Đã dừng toàn bộ tiến trình Phase 3."
    exit 0
}
trap cleanup SIGINT SIGTERM

# 3. Cấu hình tham số thực thi Phase 3 (Sweet Spot Pacing)
COMMON_ARGS="--input $INPUT_FILE --output-dir $OUTPUT_DIR --batch-size 1000 --concurrency 6 --delay-min 1.2 --delay-max 2.2 --auto-wait --max-waf-retries 0 --auto-wait-interval 60 --no-seed-existing"

echo "🚀 Đang khởi động 5 tiến trình Phase 3 song song..."

# Process 1: Batch 1 -> 17
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 1 --end-batch 17 \
    --worker-url https://tiki-proxy-worker.tyanh185.workers.dev \
    >> "$LOGS_DIR/retry_p3_1.log" 2>&1 &
P1=$!
echo "$P1" >> "$PID_FILE"
echo "  🔹 [Process 1] PID: $P1 | Batches: 0001-0017 | Worker 1 -> logs/retry_p3_1.log"

# Process 2: Batch 18 -> 34
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 18 --end-batch 34 \
    --worker-url https://tiki-proxy-worker-2.tyanh185.workers.dev \
    >> "$LOGS_DIR/retry_p3_2.log" 2>&1 &
P2=$!
echo "$P2" >> "$PID_FILE"
echo "  🔹 [Process 2] PID: $P2 | Batches: 0018-0034 | Worker 2 -> logs/retry_p3_2.log"

# Process 3: Batch 35 -> 51
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 35 --end-batch 51 \
    --worker-url https://tiki-proxy-worker-3.tyanh185.workers.dev \
    >> "$LOGS_DIR/retry_p3_3.log" 2>&1 &
P3=$!
echo "$P3" >> "$PID_FILE"
echo "  🔹 [Process 3] PID: $P3 | Batches: 0035-0051 | Worker 3 -> logs/retry_p3_3.log"

# Process 4: Batch 52 -> 68
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 52 --end-batch 68 \
    --worker-url https://tiki-proxy-worker-4.tyanh185.workers.dev \
    >> "$LOGS_DIR/retry_p3_4.log" 2>&1 &
P4=$!
echo "$P4" >> "$PID_FILE"
echo "  🔹 [Process 4] PID: $P4 | Batches: 0052-0068 | Worker 4 -> logs/retry_p3_4.log"

# Process 5: Batch 69 -> 84
$PYTHON_BIN src/main.py $COMMON_ARGS --start-batch 69 --end-batch 84 \
    --worker-url https://tiki-proxy-worker-5.tyanh185.workers.dev \
    >> "$LOGS_DIR/retry_p3_5.log" 2>&1 &
P5=$!
echo "$P5" >> "$PID_FILE"
echo "  🔹 [Process 5] PID: $P5 | Batches: 0069-0084 | Worker 5 -> logs/retry_p3_5.log"

echo "======================================================================"
echo "✅ Cả 5 tiến trình Phase 3 đã chạy ngầm thành công!"
echo "📊 Theo dõi Dashboard thời gian thực: python3 monitor/check_retry_pass3_status.py"
echo "📄 Xem live log: tail -f logs/retry_p3_1.log logs/retry_p3_2.log logs/retry_p3_3.log logs/retry_p3_4.log logs/retry_p3_5.log"
echo "🛑 Dừng toàn bộ tiến trình: ./runners/phase3/stop.sh"
echo "======================================================================"

# 4. Chờ cả 5 tiến trình hoàn thành
wait "$P1"
E1=$?
wait "$P2"
E2=$?
wait "$P3"
E3=$?
wait "$P4"
E4=$?
wait "$P5"
E5=$?

rm -f "$PID_FILE"

echo "======================================================================"
echo "🎉 [$(date '+%Y-%m-%d %H:%M:%S')] TẤT CẢ 5 TIẾN TRÌNH PHASE 3 ĐÃ HOÀN TẤT!"
echo "   Status: P1=$E1, P2=$E2, P3=$E3, P4=$E4, P5=$E5"
echo "======================================================================"

if [ $E1 -eq 0 ] && [ $E2 -eq 0 ] && [ $E3 -eq 0 ] && [ $E4 -eq 0 ] && [ $E5 -eq 0 ]; then
    exit 0
else
    exit 1
fi
