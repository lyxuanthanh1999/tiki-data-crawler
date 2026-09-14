# Auto-Convergence Multi-Phase Pipeline Implementation Plan

## Goal

Provide a one-command, end-to-end runner that lets another user clone the repository and either smoke-test or run the full crawler pipeline from Phase 1 through Phase N.

The pipeline must:

- Run Phase 1 and retry phases automatically.
- Use 5 OS processes and 5 Cloudflare Worker proxy URLs.
- Stop automatically when the failed ID set converges.
- Run the final master merge.
- Keep outputs isolated per run so test/full runs do not mix with existing production outputs.

## Key Design Decisions

### 1. Input Can Be Shared, Output Must Be Isolated

Input files may reuse existing project inputs:

- Full mode: `data/input/product_ids.txt`
- Test mode: generated from `data/input/product_ids.txt`, `data/input/product_ids_10.txt`, or another sample source

Outputs must be written under a run-specific namespace:

```text
data/output/auto_runs/<run_id>/
  phase_0001/parts/
  phase_0002/parts/
  phase_0003/parts/
  merged/

data/input/auto_runs/<run_id>/
  phase_0001_input.txt
  phase_0002_failed_ids.txt
  phase_0003_failed_ids.txt

logs/auto_runs/<run_id>/
  auto_pipeline.state.json
  phase_0001_worker_1.log
  phase_0001_worker_2.log
  ...
```

This avoids mixing smoke-test output with existing `data/output/concurrency` or `data/output/retry_*` data.

### 2. Convergence Must Compare Failed ID Sets, Not Only Counts

The stop condition should be:

```text
failed_ids_curr == failed_ids_prev
```

Implementation should store a stable hash for each phase:

```text
failed_hash = sha256("\n".join(sorted_failed_ids))
```

The pipeline may print count equality, but must decide convergence using set/hash equality.

Additional stop conditions:

- `failed_count == 0`: all input IDs resolved.
- `phase_index >= --max-phases`: safety cap reached.
- Any worker exits non-zero: stop as failed, do not declare convergence.

### 3. Smoke Test Must Still Exercise 5 Processes

For `--test N`, the runner must create at least 5 batches so all 5 processes start.

Recommended rule:

```text
batch_size = max(1, ceil(N / 5))
workers = min(5, N)
```

Examples:

- `--test 20` -> 5 workers, batch size 4
- `--test 50` -> 5 workers, batch size 10

### 4. Avoid Calling Existing Phase Scripts From Auto Mode

Do not call `runners/phase1/run.sh` from the auto pipeline. It is rigid and its paths/config are phase-specific.

Instead, implement one shared spawn path that calls:

```bash
python3 src/main.py \
  --input <phase_input> \
  --output-dir <run_output>/phase_000N/parts \
  --batch-size <batch_size> \
  --concurrency <concurrency> \
  --delay-min <delay_min> \
  --delay-max <delay_max> \
  --auto-wait \
  --max-waf-retries 0 \
  --auto-wait-interval 60 \
  --no-seed-existing \
  --start-batch <start> \
  --end-batch <end> \
  --worker-url <proxy_url>
```

This keeps full and test runs consistent.

## Proposed Files

### `runners/auto_pipeline.sh`

Thin shell wrapper around the Python orchestrator.

Responsibilities:

- Resolve project root.
- Pick `venv/bin/python` when available, otherwise `python3`.
- Exec `tools/auto_pipeline.py` with all original CLI args.

Supported CLI:

```bash
./runners/auto_pipeline.sh --test 50
./runners/auto_pipeline.sh --full --max-phases 5
./runners/auto_pipeline.sh --resume --run-id <run_id>
```

Options:

- `--test [N]`: smoke test with N IDs. Default: 50.
- `--full`: full input from `data/input/product_ids.txt`.
- `--run-id <id>`: explicit output namespace.
- `--resume`: resume an existing run from state file.
- `--max-phases <num>`: default 5.
- `--workers <num>`: default 5.
- `--phase1-concurrency <num>`: default 10.
- `--retry-concurrency <num>`: default 6.
- `--delay-min <float>`: default 1.2 for retry.
- `--delay-max <float>`: default 2.2 for retry.
- `--clean-run`: remove existing output for the same `run_id` before starting. Must refuse unless `--run-id` is explicit.

### `tools/auto_pipeline.py`

Main orchestrator.

Responsibilities:

- Prepare input file for the run.
- Split work into batch ranges.
- Spawn 5 worker processes per phase.
- Track PIDs and process exit codes.
- Run merge/reconcile after each completed phase.
- Compute failed ID set and hash.
- Decide convergence.
- Write state manifest after every important transition.
- Run final merge into run-local `merged/` output.

State file:

```json
{
  "run_id": "auto_test_20260912_181500",
  "mode": "test",
  "status": "running",
  "current_phase": 2,
  "input_file": "data/input/auto_runs/.../phase_0002_failed_ids.txt",
  "output_root": "data/output/auto_runs/...",
  "log_root": "logs/auto_runs/...",
  "phases": [
    {
      "phase": 1,
      "input_count": 50,
      "rescued_count": 31,
      "failed_count": 19,
      "failed_hash": "...",
      "exit_codes": [0, 0, 0, 0, 0],
      "started_at": "...",
      "finished_at": "..."
    }
  ]
}
```

### `tools/merge_parts.py`

Add optional arguments so auto runs can merge only the current run outputs:

```bash
python3 tools/merge_parts.py \
  --input-ids <run_input_file> \
  --parts-root data/output/auto_runs/<run_id> \
  --output-json data/output/auto_runs/<run_id>/merged/products_output.json \
  --export-csv data/output/auto_runs/<run_id>/merged/all_products.csv
```

Required changes:

- Add `--parts-root`.
- Discover parts recursively under `--parts-root`.
- Sort phase directories numerically, not lexicographically.
- Preserve current default behavior when `--parts-root` is omitted.

### `runners/stop_auto_pipeline.sh`

Stop script for the active or selected run.

Supported CLI:

```bash
./runners/stop_auto_pipeline.sh
./runners/stop_auto_pipeline.sh --run-id <run_id>
```

Responsibilities:

- Read `logs/auto_runs/<run_id>/auto_pipeline.state.json`.
- Send `SIGTERM` to orchestrator PID and active worker PIDs.
- Wait briefly.
- Report any remaining live PIDs.
- Do not remove output files.

### `monitor/check_auto_pipeline.py`

Dashboard for a run-local state file.

Supported CLI:

```bash
python3 monitor/check_auto_pipeline.py
python3 monitor/check_auto_pipeline.py --run-id <run_id>
python3 monitor/check_auto_pipeline.py --run-id <run_id> --watch
```

Display:

- Run ID, mode, current status.
- Current phase.
- 5 worker PID rows: alive, CPU, elapsed time, batch range.
- Per-phase convergence table:

```text
Phase   Input   Rescued   Failed   Failed Hash   Status
1       50      31        19       abc123...     done
2       19      0         19       abc123...     converged
```

## Implementation Steps

1. Create `tools/auto_pipeline.py`.
2. Add `runners/auto_pipeline.sh`.
3. Extend `tools/merge_parts.py` with `--parts-root`.
4. Add `runners/stop_auto_pipeline.sh`.
5. Add `monitor/check_auto_pipeline.py`.
6. Update `README.md` with the new one-command workflow.
7. Fix README references to the existing generic retry CLI:
   - Replace `--phase` examples with `--input ... --name ...`.
8. Add tests for:
   - failed ID hashing and convergence detection.
   - test-mode batch sizing creates 5 workers when possible.
   - run-local output paths do not overlap production output paths.
   - merge with `--parts-root` only reads that run's parts.

## Verification Plan

### Unit Tests

```bash
python3 -m pytest
```

### Smoke Test

```bash
./runners/auto_pipeline.sh --test 20 --run-id smoke_20
python3 monitor/check_auto_pipeline.py --run-id smoke_20
```

Expected:

- 5 worker processes are launched when possible.
- Output appears under `data/output/auto_runs/smoke_20/`.
- Production output under `data/output/concurrency/` is not modified.
- Final merged files appear under:

```text
data/output/auto_runs/smoke_20/merged/products_output.json
data/output/auto_runs/smoke_20/merged/products_output.jsonl
data/output/auto_runs/smoke_20/merged/all_products.csv
data/output/auto_runs/smoke_20/merged/products_output.failed_permanent.json
```

### Stop Test

```bash
./runners/auto_pipeline.sh --test 100 --run-id stop_test &
./runners/stop_auto_pipeline.sh --run-id stop_test
```

Expected:

- Orchestrator and active workers receive `SIGTERM`.
- State file status becomes `stopped`.
- Output files remain for inspection.

### Full Run

```bash
./runners/auto_pipeline.sh --full --max-phases 5 --run-id full_200k
```

Expected:

- Pipeline stops only when failed ID set converges, failed count reaches zero, or max phases is reached.
- Final merge output is isolated under `data/output/auto_runs/full_200k/merged/`.

## Risks And Mitigations

- WAF may keep blocking for hours: keep `--max-phases`, unlimited batch auto-wait, and clear state output.
- Worker crash may make missing IDs look like failed IDs: never declare convergence if any worker exits non-zero or phase processed count is below target.
- Test runs may pollute production outputs: all auto outputs must be run-local.
- Existing `merge_parts.py` default discovery may mix old retry dirs: auto mode must call it with `--parts-root`.
- Existing docs mention stale `--phase` CLI: update README while adding auto pipeline docs.
