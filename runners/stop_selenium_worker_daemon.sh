#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

RUN_NAME="selenium_worker_daemon"

usage() {
  cat <<'USAGE'
Usage:
  ./runners/stop_selenium_worker_daemon.sh [--name <name>]
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)
      RUN_NAME="${2:?Thiếu giá trị cho --name}"
      shift 2
      ;;
    --help|-h)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      usage
      exit 2
      ;;
  esac
done

PID_FILE="$PROJECT_DIR/logs/${RUN_NAME}.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "Không tìm thấy PID file: $PID_FILE"
  exit 1
fi

PID="$(cat "$PID_FILE" 2>/dev/null || true)"
if [[ -z "$PID" ]]; then
  echo "PID file rỗng: $PID_FILE"
  rm -f "$PID_FILE"
  exit 1
fi

if kill -0 "$PID" 2>/dev/null; then
  kill "$PID"
  echo "Đã gửi tín hiệu dừng cho '$RUN_NAME' PID $PID"
else
  echo "PID $PID không còn chạy"
fi

rm -f "$PID_FILE"
