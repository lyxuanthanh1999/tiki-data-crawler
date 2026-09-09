#!/bin/bash
# ==============================================================================
# Tiki Crawler - 5-Process Master Orchestrator (Zero-Touch Unattended)
# Tự động hóa 5 tiến trình cào song song qua 5 Cloudflare Worker Proxies
# ==============================================================================

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR" || exit 1

PYTHON_BIN="$PROJECT_DIR/venv/bin/python"
LOGS_DIR="$PROJECT_DIR/logs"
PID_FILE="$LOGS_DIR/crawler.pids"

mkdir -p "$LOGS_DIR"

echo "======================================================================"
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] Khởi động Tiki 5-Process Master Orchestrator"
echo "📂 Project Dir: $PROJECT_DIR"
echo "🐍 Python Bin: $PYTHON_BIN"
echo "======================================================================"

# 1. Kiểm tra kết nối Internet (Chờ tối đa 60s phòng trường hợp máy vừa mở nắp WiFi)
echo "🌐 Đang kiểm tra kết nối mạng..."
MAX_WAIT_NET=60
WAITED=0
while ! ping -c 1 1.1.1.1 >/dev/null 2>&1 && ! ping -c 1 8.8.8.8 >/dev/null 2>&1; do
    echo "⏳ [$(date '+%H:%M:%S')] Chưa có kết nối mạng, chờ 3 giây... (${WAITED}/${MAX_WAIT_NET}s)"
    sleep 3
    WAITED=$((WAITED + 3))
    if [ "$WAITED" -ge "$MAX_WAIT_NET" ]; then
        echo "❌ Quá thời gian chờ mạng ($MAX_WAIT_NET giây). Vui lòng kiểm tra WiFi/Internet."
        exit 1
    fi
done
echo "✅ Kết nối Internet sẵn sàng!"

# 2. Dọn dẹp PID cũ nếu có
if [ -f "$PID_FILE" ]; then
    echo "🧹 Dọn dẹp tiến trình cũ còn sót lại..."
    while read -r pid; do
        if [ -n "$pid" ] && kill -0 "$pid" >/dev/null 2>&1; then
            kill "$pid" >/dev/null 2>&1
        fi
    done < "$PID_FILE"
    rm -f "$PID_FILE"
fi

# Hàm bẫy tín hiệu tắt (SIGINT / SIGTERM)
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
    echo "🛑 Đã dừng toàn bộ tiến trình."
    exit 0
}
trap cleanup SIGINT SIGTERM

# 3. Định nghĩa các cấu hình tương ứng 5 batch trong PyCharm
COMMON_ARGS="--output-dir data/output/concurrency/parts --batch-size 1000 --concurrency 10 --delay-min 1.0 --delay-max 2.5 --auto-wait --max-waf-retries 0 --auto-wait-interval 60"

echo "🚀 Đang khởi động 5 tiến trình cào song song..."

# Process 1: Batch 1 -> 40
$PYTHON_BIN main.py $COMMON_ARGS --start-batch 1 --end-batch 40 \
    --worker-url https://tiki-proxy-worker-1.tyanh185.workers.dev \
    >> "$LOGS_DIR/p1.log" 2>&1 &
P1=$!
echo "$P1" >> "$PID_FILE"
echo "  🔹 [Process 1] PID: $P1 | Batches: 0001-0040 | Worker 1 -> logs/p1.log"

# Process 2: Batch 41 -> 80
$PYTHON_BIN main.py $COMMON_ARGS --start-batch 41 --end-batch 80 \
    --worker-url https://tiki-proxy-worker-2.tyanh185.workers.dev \
    >> "$LOGS_DIR/p2.log" 2>&1 &
P2=$!
echo "$P2" >> "$PID_FILE"
echo "  🔹 [Process 2] PID: $P2 | Batches: 0041-0080 | Worker 2 -> logs/p2.log"

# Process 3: Batch 81 -> 120
$PYTHON_BIN main.py $COMMON_ARGS --start-batch 81 --end-batch 120 \
    --worker-url https://tiki-proxy-worker-3.tyanh185.workers.dev \
    >> "$LOGS_DIR/p3.log" 2>&1 &
P3=$!
echo "$P3" >> "$PID_FILE"
echo "  🔹 [Process 3] PID: $P3 | Batches: 0081-0120 | Worker 3 -> logs/p3.log"

# Process 4: Batch 121 -> 160
$PYTHON_BIN main.py $COMMON_ARGS --start-batch 121 --end-batch 160 \
    --worker-url https://tiki-proxy-worker-4.tyanh185.workers.dev \
    >> "$LOGS_DIR/p4.log" 2>&1 &
P4=$!
echo "$P4" >> "$PID_FILE"
echo "  🔹 [Process 4] PID: $P4 | Batches: 0121-0160 | Worker 4 -> logs/p4.log"

# Process 5: Batch 161 -> 200
$PYTHON_BIN main.py $COMMON_ARGS --start-batch 161 --end-batch 200 \
    --worker-url https://tiki-proxy-worker-5.tyanh185.workers.dev \
    >> "$LOGS_DIR/p5.log" 2>&1 &
P5=$!
echo "$P5" >> "$PID_FILE"
echo "  🔹 [Process 5] PID: $P5 | Batches: 0161-0200 | Worker 5 -> logs/p5.log"

echo "======================================================================"
echo "✅ Cả 5 tiến trình đã chạy ngầm thành công!"
echo "📊 Theo dõi log thời gian thực: tail -f logs/p1.log logs/p2.log logs/p3.log logs/p4.log logs/p5.log"
echo "🛑 Dừng toàn bộ tiến trình: ./stop_all.sh"
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
echo "🎉 [$(date '+%Y-%m-%d %H:%M:%S')] TẤT CẢ 5 TIẾN TRÌNH ĐÃ HOÀN TẤT!"
echo "   Status: P1=$E1, P2=$E2, P3=$E3, P4=$E4, P5=$E5"
echo "======================================================================"

# Nếu tất cả exit code = 0 thì trả về 0 để daemon biết là đã hoàn tất hoàn toàn
if [ $E1 -eq 0 ] && [ $E2 -eq 0 ] && [ $E3 -eq 0 ] && [ $E4 -eq 0 ] && [ $E5 -eq 0 ]; then
    exit 0
else
    exit 1
fi
