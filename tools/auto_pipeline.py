#!/usr/bin/env python3
"""Run isolated crawler phases until the failed-ID set converges."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROXY_URLS = [
    "https://tiki-proxy-worker.tyanh185.workers.dev",
    "https://tiki-proxy-worker-2.tyanh185.workers.dev",
    "https://tiki-proxy-worker-3.tyanh185.workers.dev",
    "https://tiki-proxy-worker-4.tyanh185.workers.dev",
    "https://tiki-proxy-worker-5.tyanh185.workers.dev",
]


def timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_ids(path: Path) -> list[str]:
    if path.suffix.lower() == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        values = data.keys() if isinstance(data, dict) else [item.get("id") for item in data]
        ids = {str(value) for value in values if str(value).isdigit()}
    else:
        ids = {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip().isdigit()}
    return sorted(ids, key=int)


def write_ids(path: Path, ids: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(ids) + ("\n" if ids else ""), encoding="utf-8")


def ids_hash(ids: list[str]) -> str:
    return hashlib.sha256("\n".join(sorted(ids, key=int)).encode()).hexdigest()


def save_state(path: Path, state: dict) -> None:
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    temp.replace(path)


def load_state(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def merge_run(run_root: Path, source_input: Path) -> tuple[list[str], int]:
    merged = run_root / "merged"
    command = [
        sys.executable, str(ROOT / "tools/merge_parts.py"),
        "--parts-root", str(run_root), "--input-ids", str(source_input),
        "--output-json", str(merged / "products_output.json"),
        "--export-csv", str(merged / "all_products.csv"),
    ]
    result = subprocess.run(command, cwd=ROOT, check=False)
    failed_file = merged / "products_output.failed_permanent.json"
    if result.returncode != 0 or not failed_file.exists():
        return [], result.returncode or 1
    return read_ids(failed_file), 0


def incomplete_ids(parts: Path, input_ids: list[str]) -> list[str]:
    """Return IDs that have neither a product, permanent failure, nor retry state."""
    accounted_for = set()
    for path in parts.glob("products_part_*.jsonl"):
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                product_id = json.loads(line).get("id")
            except (json.JSONDecodeError, AttributeError):
                continue
            if product_id is not None:
                accounted_for.add(str(product_id))
    for path in parts.glob("products_part_*.json"):
        if any(path.name.endswith(suffix) for suffix in (".retry.json", ".failed_permanent.json", ".stats.json")):
            continue
        try:
            products = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(products, list):
            accounted_for.update(str(item["id"]) for item in products if isinstance(item, dict) and item.get("id") is not None)
    for pattern in ("products_part_*.failed_permanent.json",):
        for path in parts.glob(pattern):
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(data, dict):
                accounted_for.update(str(key) for key in data)
    return sorted(set(input_ids) - accounted_for, key=int)


def run_phase(args: argparse.Namespace, state_path: Path, run_root: Path, log_root: Path,
              phase: int, input_file: Path, input_count: int) -> list[int]:
    parts = run_root / f"phase_{phase:04d}" / "parts"
    parts.mkdir(parents=True, exist_ok=True)
    batch_size = max(1, math.ceil(input_count / args.workers))
    total_batches = math.ceil(input_count / batch_size)
    worker_count = min(args.workers, total_batches)
    batches_per_worker = math.ceil(total_batches / worker_count)
    processes = []
    workers = []
    for index in range(worker_count):
        start = index * batches_per_worker + 1
        end = min(total_batches, (index + 1) * batches_per_worker)
        log_path = log_root / f"phase_{phase:04d}_worker_{index + 1}.log"
        command = [
            sys.executable, str(ROOT / "src/main.py"), "--input", str(input_file),
            "--output-dir", str(parts), "--batch-size", str(batch_size),
            "--concurrency", str(args.phase1_concurrency if phase == 1 else args.retry_concurrency),
            "--delay-min", str(args.delay_min), "--delay-max", str(args.delay_max),
            "--auto-wait", "--max-waf-retries", "0", "--auto-wait-interval", "60",
            "--no-seed-existing", "--start-batch", str(start), "--end-batch", str(end),
            "--worker-url", PROXY_URLS[index % len(PROXY_URLS)],
        ]
        handle = log_path.open("a", encoding="utf-8")
        process = subprocess.Popen(command, cwd=ROOT, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
        processes.append((process, handle))
        workers.append({"worker": index + 1, "pid": process.pid, "start_batch": start, "end_batch": end})
    state = load_state(state_path)
    state["current_phase"] = phase
    state["active_workers"] = workers
    state["batch_size"] = batch_size
    state["total_batches"] = total_batches
    save_state(state_path, state)
    exit_codes = []
    for process, handle in processes:
        exit_codes.append(process.wait())
        handle.close()
    return exit_codes


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto-convergence multi-phase crawler")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--test", nargs="?", const=50, type=int, metavar="N")
    modes.add_argument("--full", action="store_true")
    parser.add_argument("--input", type=Path, default=ROOT / "data/input/product_ids.txt")
    parser.add_argument("--run-id")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--max-phases", type=int, default=5)
    parser.add_argument("--workers", type=int, default=5)
    parser.add_argument("--phase1-concurrency", type=int, default=10)
    parser.add_argument("--retry-concurrency", type=int, default=6)
    parser.add_argument("--delay-min", type=float, default=1.2)
    parser.add_argument("--delay-max", type=float, default=2.2)
    parser.add_argument("--clean-run", action="store_true", help="Xóa đúng namespace của run_id trước khi chạy lại")
    args = parser.parse_args()
    if args.run_id and not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", args.run_id):
        parser.error("--run-id chỉ được chứa chữ, số, '.', '_' hoặc '-'")
    if args.test is None and not args.full and not args.resume:
        parser.error("Chọn --test N, --full hoặc --resume --run-id <id>")
    if args.test is not None and args.test < 1:
        parser.error("--test N phải lớn hơn 0")
    if args.max_phases < 1 or args.workers < 1:
        parser.error("--max-phases và --workers phải lớn hơn 0")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("Cần 0 <= --delay-min <= --delay-max")
    if args.clean_run and args.resume:
        parser.error("--clean-run không dùng chung với --resume")
    return args


def main() -> int:
    args = parse_args()
    run_id = args.run_id or f"auto_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    run_root = ROOT / "data/output/auto_runs" / run_id
    input_root = ROOT / "data/input/auto_runs" / run_id
    log_root = ROOT / "logs/auto_runs" / run_id
    state_path = log_root / "auto_pipeline.state.json"
    if args.clean_run:
        if not args.run_id:
            raise SystemExit("--clean-run yêu cầu --run-id để tránh xóa nhầm run khác")
        for path in (run_root, input_root, log_root):
            if path.exists():
                shutil.rmtree(path)
    for path in (run_root, input_root, log_root):
        path.mkdir(parents=True, exist_ok=True)
    if args.resume:
        if not state_path.exists():
            raise SystemExit(f"Không tìm thấy state để resume: {state_path}")
        state = load_state(state_path)
        source_input = Path(state["source_input"])
        current_input = Path(state["input_file"])
        phase = int(state["current_phase"])
    else:
        source_input = args.input.resolve()
        if not source_input.exists():
            raise SystemExit(f"Không tìm thấy input: {source_input}")
        ids = read_ids(source_input)
        if args.test is not None:
            ids = ids[:args.test]
        current_input = input_root / "phase_0001_input.txt"
        write_ids(current_input, ids)
        state = {"run_id": run_id, "mode": "test" if args.test is not None else "full", "status": "running", "orchestrator_pid": os.getpid(), "source_input": str(source_input), "input_file": str(current_input), "output_root": str(run_root), "log_root": str(log_root), "current_phase": 1, "phases": []}
        save_state(state_path, state)
        phase = 1
    previous_hash = state["phases"][-1].get("failed_hash") if state["phases"] else None
    while phase <= args.max_phases:
        input_ids = read_ids(current_input)
        if not input_ids:
            state["status"] = "completed"
            break
        started = timestamp()
        exit_codes = run_phase(args, state_path, run_root, log_root, phase, current_input, len(input_ids))
        failed, merge_code = merge_run(run_root, source_input)
        current_hash = ids_hash(failed)
        phase_incomplete = incomplete_ids(run_root / f"phase_{phase:04d}" / "parts", input_ids)
        state = load_state(state_path)
        state["phases"].append({"phase": phase, "input_count": len(input_ids), "rescued_count": len(input_ids) - len(failed), "failed_count": len(failed), "failed_hash": current_hash, "incomplete_count": len(phase_incomplete), "exit_codes": exit_codes, "merge_exit_code": merge_code, "started_at": started, "finished_at": timestamp()})
        save_state(state_path, state)
        if any(code != 0 for code in exit_codes) or merge_code != 0 or phase_incomplete:
            state["status"] = "failed"
            state["failure_reason"] = "phase_incomplete" if phase_incomplete else "worker_or_merge_error"
            break
        if not failed:
            state["status"] = "completed"
            break
        if previous_hash == current_hash:
            state["status"] = "converged"
            break
        previous_hash = current_hash
        phase += 1
        current_input = input_root / f"phase_{phase:04d}_input.txt"
        write_ids(current_input, failed)
        state["current_phase"] = phase
        state["input_file"] = str(current_input)
        save_state(state_path, state)
    else:
        state = load_state(state_path)
        state["status"] = "max_phases_reached"
    state["active_workers"] = []
    state["finished_at"] = timestamp()
    save_state(state_path, state)
    print(json.dumps({"run_id": run_id, "status": state["status"], "state": str(state_path), "merged": str(run_root / "merged")}, ensure_ascii=False, indent=2))
    return 0 if state["status"] in {"completed", "converged", "max_phases_reached"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
