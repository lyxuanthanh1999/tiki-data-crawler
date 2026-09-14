#!/usr/bin/env bash
set -euo pipefail
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --run-id) RUN_ID="$2"; shift 2 ;;
    -h|--help) echo "Usage: $0 [--run-id RUN_ID]"; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; exit 1 ;;
  esac
done
if [[ -z "$RUN_ID" ]]; then
  STATE="$(find "$PROJECT_DIR/logs/auto_runs" -name auto_pipeline.state.json -print 2>/dev/null | sort | tail -1)"
else
  STATE="$PROJECT_DIR/logs/auto_runs/$RUN_ID/auto_pipeline.state.json"
fi
[[ -f "$STATE" ]] || { echo "Không tìm thấy state: $STATE" >&2; exit 1; }
python3 - "$STATE" <<'PY'
import json
import os
import signal
import sys

path = sys.argv[1]
with open(path, encoding="utf-8") as handle:
    state = json.load(handle)
pids = [state.get("orchestrator_pid")]
pids.extend(worker.get("pid") for worker in state.get("active_workers", []))
for pid in {int(pid) for pid in pids if pid}:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
state["status"] = "stopped"
state["active_workers"] = []
with open(path, "w", encoding="utf-8") as handle:
    json.dump(state, handle, ensure_ascii=False, indent=2)
print(f"Đã gửi SIGTERM cho run {state.get('run_id')}")
PY
