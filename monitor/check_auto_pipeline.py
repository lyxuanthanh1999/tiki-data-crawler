#!/usr/bin/env python3
"""Print the latest state of an auto-convergence run."""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor auto-convergence pipeline")
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()
    path = ROOT / "logs/auto_runs" / args.run_id / "auto_pipeline.state.json"
    if not path.exists():
        parser.error(f"Không tìm thấy state: {path}")
    state = json.loads(path.read_text(encoding="utf-8"))
    print(f"Run: {state['run_id']} | Status: {state['status']} | Phase: {state.get('current_phase', '-')}")
    for phase in state.get("phases", []):
        print(f"Phase {phase['phase']}: input={phase['input_count']:,} rescued={phase['rescued_count']:,} failed={phase['failed_count']:,} exit={phase['exit_codes']}")
    for worker in state.get("active_workers", []):
        print(f"Worker {worker['worker']}: PID={worker['pid']} batches={worker['start_batch']}-{worker['end_batch']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
