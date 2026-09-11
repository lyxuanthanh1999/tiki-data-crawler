#!/usr/bin/env bash
# ==============================================================================
# Tiki Generic Retry Crawler
# Chạy ngầm 5 tiến trình song song để vớt sản phẩm từ bất kỳ file ID lỗi nào.
#
# CÁCH DÙNG:
#   ./runners/retry.sh --input <file>
#   ./runners/retry.sh --input <file> --name <tên> --workers 5 --concurrency 6
#
# INPUT FILE hỗ trợ:
#   - .txt   : mỗi dòng 1 product_id
#   - .json  : dạng failed_permanent.json  {"id": {"reason": ...}}
#              hoặc dạng array             [{"id": 123, ...}, ...]
#
# VÍ DỤ:
#   ./runners/retry.sh --input data/output/concurrency/products_output.failed_permanent.json
#   ./runners/retry.sh --input data/input/my_ids.txt --name pass4
# ==============================================================================

set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# ── Defaults ──────────────────────────────────────────────────────────────────
INPUT_FILE=""
RUN_NAME=""
NUM_WORKERS=5
BATCH_SIZE=1000
CONCURRENCY=6
DELAY_MIN=1.2
DELAY_MAX=2.2
WORKER_URLS=(
    "https://tiki-proxy-worker.tyanh185.workers.dev"
    "https://tiki-proxy-worker-2.tyanh185.workers.dev"
    "https://tiki-proxy-worker-3.tyanh185.workers.dev"
    "https://tiki-proxy-worker-4.tyanh185.workers.dev"
    "https://tiki-proxy-worker-5.tyanh185.workers.dev"
)

# ── Parse args ────────────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --input)    INPUT_FILE="$2";   shift 2 ;;
        --name)     RUN_NAME="$2";     shift 2 ;;
        --workers)  NUM_WORKERS="$2";  shift 2 ;;
        --concurrency) CONCURRENCY="$2"; shift 2 ;;
        --batch-size)  BATCH_SIZE="$2";  shift 2 ;;
        --delay-min)   DELAY_MIN="$2";   shift 2 ;;
        --delay-max)   DELAY_MAX="$2";   shift 2 ;;
        --help|-h)
            grep '^#' "$0" | head -20 | sed 's/^# \?//'
            exit 0
            ;;
        *) echo "❌ Unknown arg: $1"; exit 1 ;;
    esac
done

# ── Validate input ─────────────────────────────────────────────────────────────
if [[ -z "$INPUT_FILE" ]]; then
    echo "❌ Thiếu --input <file>"
    echo "   Ví dụ: ./runners/retry.sh --input data/output/concurrency/products_output.failed_permanent.json"
    exit 1
fi

if [[ ! -f "$INPUT_FILE" ]]; then
    echo "❌ File không tồn tại: $INPUT_FILE"
    exit 1
fi

# ── Auto-detect run name từ tên file ──────────────────────────────────────────
if [[ -z "$RUN_NAME" ]]; then
    BASE="$(basename "$INPUT_FILE" | sed 's/\.[^.]*$//' | tr '.' '_' | tr '-' '_')"
    RUN_NAME="$BASE"
fi

LOGS_DIR="$PROJECT_DIR/logs"
OUTPUT_DIR="$PROJECT_DIR/data/output/retry_${RUN_NAME}/parts"
TXT_FILE="$PROJECT_DIR/data/input/retry_${RUN_NAME}.txt"
PID_FILE="$LOGS_DIR/retry_${RUN_NAME}.pids"
RUN_CONFIG="$LOGS_DIR/retry_${RUN_NAME}.config.json"

PYTHON_BIN="$PROJECT_DIR/venv/bin/python"
[[ -f "$PYTHON_BIN" ]] || PYTHON_BIN="$(which python3)"

mkdir -p "$LOGS_DIR" "$OUTPUT_DIR" "$(dirname "$TXT_FILE")"

echo "======================================================================"
echo "🚀 [$(date '+%Y-%m-%d %H:%M:%S')] GENERIC RETRY CRAWLER"
echo "   Run name  : $RUN_NAME"
echo "   Input file: $INPUT_FILE"
echo "   Output dir: $OUTPUT_DIR"
echo "======================================================================"

# ── Bước 1: Chuẩn hóa input → txt ─────────────────────────────────────────────
echo "🔄 Chuẩn hóa input file → $TXT_FILE ..."

EXT="${INPUT_FILE##*.}"
if [[ "$EXT" == "json" ]]; then
    "$PYTHON_BIN" - <<PYEOF
import json, sys

with open("$INPUT_FILE") as f:
    raw = json.load(f)

ids = []
if isinstance(raw, dict):
    # {"id": {"reason": ...}} hoặc {"id": "reason"} format
    ids = [str(k) for k in raw.keys() if str(k).isdigit()]
elif isinstance(raw, list):
    # [{"id": 123, ...}] format
    ids = [str(p["id"]) for p in raw if isinstance(p, dict) and p.get("id")]

ids = sorted(set(ids), key=int)
with open("$TXT_FILE", "w") as f:
    f.write("\n".join(ids) + "\n")

print(f"✅ Extracted {len(ids):,} IDs từ JSON → $TXT_FILE")
PYEOF
else
    # .txt — copy trực tiếp (lọc bỏ dòng không phải số)
    grep -E '^[0-9]+$' "$INPUT_FILE" | sort -n | uniq > "$TXT_FILE"
    echo "✅ Copied $(wc -l < "$TXT_FILE" | tr -d ' ') IDs từ TXT → $TXT_FILE"
fi

TOTAL_IDS=$(wc -l < "$TXT_FILE" | tr -d ' ')
if [[ "$TOTAL_IDS" -eq 0 ]]; then
    echo "❌ File không chứa ID nào hợp lệ."
    exit 1
fi

# ── Bước 2: Tính tổng số batch và phân chia cho workers ───────────────────────
TOTAL_BATCHES=$(( (TOTAL_IDS + BATCH_SIZE - 1) / BATCH_SIZE ))
BATCHES_PER_WORKER=$(( (TOTAL_BATCHES + NUM_WORKERS - 1) / NUM_WORKERS ))

echo ""
echo "📊 Thống kê:"
echo "   Total IDs   : $TOTAL_IDS"
echo "   Batch size  : $BATCH_SIZE"
echo "   Total batches: $TOTAL_BATCHES"
echo "   Workers     : $NUM_WORKERS (≈ $BATCHES_PER_WORKER batch/worker)"
echo ""

# ── Bước 3: Kiểm tra kết nối mạng ────────────────────────────────────────────
echo "🌐 Đang kiểm tra kết nối mạng..."
MAX_WAIT_NET=60; WAITED=0
while ! ping -c 1 1.1.1.1 >/dev/null 2>&1 && ! ping -c 1 8.8.8.8 >/dev/null 2>&1; do
    sleep 3; WAITED=$((WAITED + 3))
    if [[ $WAITED -ge $MAX_WAIT_NET ]]; then
        echo "❌ Không có kết nối mạng sau ${MAX_WAIT_NET}s."
        exit 1
    fi
done
echo "✅ Kết nối Internet sẵn sàng!"

# ── Bước 4: Dọn PID cũ ────────────────────────────────────────────────────────
if [[ -f "$PID_FILE" ]]; then
    while read -r pid; do
        [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null && kill "$pid" 2>/dev/null || true
    done < "$PID_FILE"
    rm -f "$PID_FILE"
fi

# ── Bước 5: Khởi động workers ─────────────────────────────────────────────────
COMMON_ARGS="--input $TXT_FILE --output-dir $OUTPUT_DIR --batch-size $BATCH_SIZE --concurrency $CONCURRENCY --delay-min $DELAY_MIN --delay-max $DELAY_MAX --auto-wait --max-waf-retries 0 --auto-wait-interval 60 --no-seed-existing"

echo "🚀 Đang khởi động $NUM_WORKERS tiến trình song song..."
echo ""

PIDS=()
for i in $(seq 1 $NUM_WORKERS); do
    START_BATCH=$(( (i - 1) * BATCHES_PER_WORKER + 1 ))
    END_BATCH=$(( i * BATCHES_PER_WORKER ))
    [[ $END_BATCH -gt $TOTAL_BATCHES ]] && END_BATCH=$TOTAL_BATCHES
    [[ $START_BATCH -gt $TOTAL_BATCHES ]] && break

    WORKER_IDX=$(( (i - 1) % ${#WORKER_URLS[@]} ))
    WORKER_URL="${WORKER_URLS[$WORKER_IDX]}"
    LOG_FILE="$LOGS_DIR/retry_${RUN_NAME}_${i}.log"

    $PYTHON_BIN src/main.py $COMMON_ARGS \
        --start-batch $START_BATCH \
        --end-batch $END_BATCH \
        --worker-url "$WORKER_URL" \
        >> "$LOG_FILE" 2>&1 &

    PID=$!
    echo "$PID" >> "$PID_FILE"
    PIDS+=($PID)
    echo "  🔹 [Worker $i] PID: $PID | Batch $START_BATCH–$END_BATCH | → retry_${RUN_NAME}_${i}.log"
done

# ── Lưu config để monitor đọc ─────────────────────────────────────────────────
cat > "$RUN_CONFIG" <<JSON
{
  "run_name": "$RUN_NAME",
  "input_file": "$TXT_FILE",
  "output_dir": "$OUTPUT_DIR",
  "total_ids": $TOTAL_IDS,
  "total_batches": $TOTAL_BATCHES,
  "num_workers": $NUM_WORKERS,
  "batches_per_worker": $BATCHES_PER_WORKER,
  "batch_size": $BATCH_SIZE,
  "started_at": "$(date '+%Y-%m-%d %H:%M:%S')",
  "pid_file": "$PID_FILE",
  "log_prefix": "$LOGS_DIR/retry_${RUN_NAME}"
}
JSON

echo ""
echo "======================================================================"
echo "✅ $NUM_WORKERS tiến trình đã chạy ngầm!"
echo ""
echo "📊 Theo dõi: python3 monitor/check_retry.py --name $RUN_NAME"
echo "📄 Live log: tail -f logs/retry_${RUN_NAME}_*.log"
echo "🛑 Dừng    : ./runners/stop_retry.sh --name $RUN_NAME"
echo "======================================================================"

# Chờ tất cả workers xong (nếu chạy foreground)
EXIT_CODES=()
for PID in "${PIDS[@]}"; do
    wait "$PID" && EXIT_CODES+=(0) || EXIT_CODES+=($?)
done

rm -f "$PID_FILE"

echo ""
echo "======================================================================"
echo "🎉 [$(date '+%Y-%m-%d %H:%M:%S')] TẤT CẢ WORKERS ĐÃ HOÀN TẤT!"
echo "   Exit codes: ${EXIT_CODES[*]}"
echo "======================================================================"
