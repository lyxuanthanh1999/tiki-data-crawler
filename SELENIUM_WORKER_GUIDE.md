# Selenium Worker Hybrid Guide

Guide này dành riêng cho nhánh `selenium-worker-hybrid`.

## Mục Tiêu

Chạy crawl Tiki theo mô hình:

- Selenium chỉ dùng để lấy browser session/cookie.
- `aiohttp` vẫn là engine crawl chính.
- Cloudflare Worker là proxy tùy chọn cho một phần input.
- Daemon runner chạy nền, tự resume theo file output/retry/progress.

## 1. Capture Cookie Bằng Selenium

```bash
./venv/bin/python tools/capture_tiki_browser_session.py \
  --output data/session/tiki_browser_session.json \
  --worker-url https://tiki-proxy-worker.tyanh185.workers.dev \
  --wait-seconds 8
```

Output:

```text
data/session/tiki_browser_session.json
```

File này chứa cookie local, không push GitHub.

## 2. Chạy Nền Part 1: Tiki Direct

```bash
./runners/run_selenium_worker_daemon.sh --name tiki_part1_direct -- \
  --input data/input/product_ids_part1.txt \
  --output-dir data/output/selenium_worker_test/part1_parts \
  --batch-size 1000 \
  --concurrency 10 \
  --delay-min 0.8 \
  --delay-max 2.0 \
  --cookie-file data/session/tiki_browser_session.json
```

## 3. Chạy Nền Part 2: Cloudflare Worker

```bash
./runners/run_selenium_worker_daemon.sh --name tiki_worker -- \
  --input data/input/product_ids_part2.txt \
  --output-dir data/output/selenium_worker_test/parts \
  --batch-size 1000 \
  --concurrency 10 \
  --delay-min 0.8 \
  --delay-max 2.0 \
  --worker-url https://tiki-proxy-worker.tyanh185.workers.dev \
  --cookie-file data/session/tiki_browser_session.json
```

Không cho 2 process ghi cùng `--output-dir`.

## 4. Dừng Từng Daemon

Dừng part1:

```bash
./runners/stop_selenium_worker_daemon.sh --name tiki_part1_direct
```

Dừng part2:

```bash
./runners/stop_selenium_worker_daemon.sh --name tiki_worker
```

## 5. Kiểm Tra Process Đang Chạy

```bash
ls -l logs/tiki_part1_direct.pid logs/tiki_worker.pid 2>/dev/null
```

```bash
ps aux | grep -E "tiki_part1_direct|tiki_worker|src/main.py|caffeinate" | grep -v grep
```

## 6. Xem Log

```bash
tail -f logs/tiki_part1_direct.log
```

```bash
tail -f logs/tiki_worker.log
```

## 7. Kiểm Tra Output Có Tăng Không

Part1:

```bash
wc -l data/output/selenium_worker_test/part1_parts/products_part_0001.jsonl
stat -f '%Sm %N' data/output/selenium_worker_test/part1_parts/products_part_0001.jsonl
```

Part2:

```bash
wc -l data/output/selenium_worker_test/parts/products_part_0009.jsonl
stat -f '%Sm %N' data/output/selenium_worker_test/parts/products_part_0009.jsonl
```

## 8. Đếm Tổng Kết Quả

```bash
./venv/bin/python - <<'PY'
import json, time
from pathlib import Path

roots = [
    ("part1_direct", Path("data/output/selenium_worker_test/part1_parts"), 100000),
    ("part2_worker", Path("data/output/selenium_worker_test/parts"), 100000),
]

grand_ok = grand_perm = grand_retry = 0
for label, root, expected in roots:
    ok = perm = retry = 0
    cooldowns = []
    latest = []

    for jp in sorted(root.glob("products_part_*.jsonl")):
        count = sum(1 for line in jp.open(encoding="utf-8") if line.strip())
        ok += count
        latest.append((jp.stat().st_mtime, jp.name, count))

    for fp in sorted(root.glob("products_part_*.failed_permanent.json")):
        try:
            perm += len(json.loads(fp.read_text(encoding="utf-8")))
        except Exception:
            pass

    for rp in sorted(root.glob("products_part_*.retry.json")):
        try:
            data = json.loads(rp.read_text(encoding="utf-8"))
        except Exception:
            continue
        retry += len([key for key in data if key != "_global"])
        global_state = data.get("_global")
        if global_state and global_state.get("next_retry_at"):
            remaining = max(0, int(float(global_state["next_retry_at"]) - time.time() + 0.999))
            cooldowns.append((rp.name, remaining, global_state.get("reason")))

    done = ok + perm
    grand_ok += ok
    grand_perm += perm
    grand_retry += retry

    print(f"{label}: ok={ok}, permanent={perm}, retry={retry}, done={done}, remaining_est={expected - done}")
    print(f"  cooldowns={cooldowns[:5]}")
    for mtime, name, count in sorted(latest)[-3:]:
        print(f"  latest={time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(mtime))} {name} lines={count}")

print("TOTAL:")
print(f"  ok={grand_ok}, permanent={grand_perm}, retry={grand_retry}, done={grand_ok + grand_perm}, remaining_est={200000 - (grand_ok + grand_perm)}")
PY
```

## 9. Resume

Chạy lại đúng lệnh cũ, giữ nguyên:

- `--input`
- `--output-dir`
- `--batch-size`

Crawler sẽ bỏ qua ID đã thành công trong `.progress.txt`, giữ lỗi tạm thời trong `.retry.json`, và tiếp tục các ID đến hạn.

## 10. WAF Backoff

Khi gặp HTML challenge/WAF:

```text
5 phút -> 15 phút -> 30 phút -> 1 tiếng -> tiếp tục 1 tiếng
```

Nếu daemon còn chạy, `main.py` sẽ tự chờ và resume sau cooldown.
