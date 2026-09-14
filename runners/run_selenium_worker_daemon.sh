#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

PYTHON_BIN="$PROJECT_DIR/venv/bin/python"
[[ -x "$PYTHON_BIN" ]] || PYTHON_BIN="$(command -v python3)"
export PYTHONUNBUFFERED=1

RUN_NAME="selenium_worker_daemon"
RESUME_INTERVAL=300
USE_CAFFEINATE=1
FOREGROUND=0
MAIN_ARGS=()

usage() {
  cat <<'USAGE'
Usage:
  ./runners/run_selenium_worker_daemon.sh [runner options] -- [main.py options]

Runner options:
  --name <name>              Tên log/pid file. Mặc định: selenium_worker_daemon
  --resume-interval <sec>    Sau khi main.py thoát, ngủ bao lâu rồi chạy lại. Mặc định: 300
  --no-caffeinate            Không dùng caffeinate để giữ macOS thức
  --foreground               Chạy vòng daemon ở foreground (dùng nội bộ bởi nohup)

Ví dụ:
  ./runners/run_selenium_worker_daemon.sh --name tiki_worker -- \
    --input data/input/product_ids_part2.txt \
    --output-dir data/output/selenium_worker_test/parts \
    --batch-size 1000 \
    --concurrency 10 \
    --delay-min 0.8 \
    --delay-max 2.0 \
    --worker-url https://tiki-proxy-worker.tyanh185.workers.dev \
    --cookie-file data/session/tiki_browser_session.json
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)
      RUN_NAME="${2:?Thiếu giá trị cho --name}"
      shift 2
      ;;
    --resume-interval)
      RESUME_INTERVAL="${2:?Thiếu giá trị cho --resume-interval}"
      shift 2
      ;;
    --no-caffeinate)
      USE_CAFFEINATE=0
      shift
      ;;
    --foreground)
      FOREGROUND=1
      shift
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    --)
      shift
      MAIN_ARGS=("$@")
      break
      ;;
    *)
      echo "Unknown runner option: $1"
      usage
      exit 2
      ;;
  esac
done

if [[ ${#MAIN_ARGS[@]} -eq 0 ]]; then
  echo "Thiếu main.py options sau dấu --"
  usage
  exit 2
fi

mkdir -p "$PROJECT_DIR/logs"
LOG_FILE="$PROJECT_DIR/logs/${RUN_NAME}.log"
PID_FILE="$PROJECT_DIR/logs/${RUN_NAME}.pid"

if [[ "$FOREGROUND" -eq 0 && -f "$PID_FILE" ]]; then
  OLD_PID="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$OLD_PID" ]] && kill -0 "$OLD_PID" 2>/dev/null; then
    echo "Run '$RUN_NAME' đang chạy với PID $OLD_PID"
    echo "Log: $LOG_FILE"
    exit 1
  fi
fi

if [[ "$FOREGROUND" -eq 0 ]]; then
  RUNNER_ARGS=(--foreground --name "$RUN_NAME" --resume-interval "$RESUME_INTERVAL")
  if [[ "$USE_CAFFEINATE" -eq 0 ]]; then
    RUNNER_ARGS+=(--no-caffeinate)
  fi

  nohup "$0" "${RUNNER_ARGS[@]}" -- "${MAIN_ARGS[@]}" >> "$LOG_FILE" 2>&1 &
  DAEMON_PID=$!
  echo "$DAEMON_PID" > "$PID_FILE"

  echo "Đã chạy ngầm: $RUN_NAME"
  echo "PID: $DAEMON_PID"
  echo "Log: $LOG_FILE"
  echo "Theo dõi: tail -f \"$LOG_FILE\""
  echo "Dừng: ./runners/stop_selenium_worker_daemon.sh --name \"$RUN_NAME\""
  exit 0
fi

if [[ "$USE_CAFFEINATE" -eq 1 ]] && command -v caffeinate >/dev/null 2>&1; then
  WAKE_WRAPPER=(caffeinate -dimsu)
else
  WAKE_WRAPPER=()
fi

trap 'echo "[$(date "+%Y-%m-%d %H:%M:%S %z")] Stop daemon"; exit 0' TERM INT

echo "======================================================================"
echo "Start daemon: $(date '+%Y-%m-%d %H:%M:%S %z')"
echo "Run name    : $RUN_NAME"
echo "Python      : $PYTHON_BIN"
echo "Log         : $LOG_FILE"
echo "PID file    : $PID_FILE"
echo "Resume every: ${RESUME_INTERVAL}s"
echo "Caffeinate  : $([[ ${#WAKE_WRAPPER[@]} -gt 0 ]] && echo on || echo off)"
echo "======================================================================"

while true; do
  echo
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] Run main.py"
  set +e
  if [[ ${#WAKE_WRAPPER[@]} -gt 0 ]]; then
    "${WAKE_WRAPPER[@]}" "$PYTHON_BIN" "$PROJECT_DIR/src/main.py" \
      --auto-wait \
      --auto-wait-interval 300 \
      --max-waf-retries 0 \
      "${MAIN_ARGS[@]}"
  else
    "$PYTHON_BIN" "$PROJECT_DIR/src/main.py" \
      --auto-wait \
      --auto-wait-interval 300 \
      --max-waf-retries 0 \
      "${MAIN_ARGS[@]}"
  fi
  EXIT_CODE=$?
  set -e
  echo "[$(date '+%Y-%m-%d %H:%M:%S %z')] main.py exited with code $EXIT_CODE"
  echo "Sleep ${RESUME_INTERVAL}s before resume scan..."
  sleep "$RESUME_INTERVAL"
done
