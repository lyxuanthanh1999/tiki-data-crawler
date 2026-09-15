# Selenium Worker Hybrid Guide

Guide này dành riêng cho nhánh `selenium-worker-hybrid`.

## Mục Tiêu

Chạy crawl Tiki theo mô hình:

- Selenium chỉ dùng để lấy browser session/cookie.
- `aiohttp` vẫn là engine crawl chính.
- Cloudflare Worker là proxy chính; có thể truyền nhiều Worker URL để chia tải.
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

## 2. Danh Sách Cloudflare Worker

File `data/input/worker_urls.txt` đang chứa 5 Worker endpoint:

```text
https://tiki-proxy-worker.tyanh185.workers.dev
https://tiki-proxy-worker-2.tyanh185.workers.dev
https://tiki-proxy-worker-3.tyanh185.workers.dev
https://tiki-proxy-worker-4.tyanh185.workers.dev
https://tiki-proxy-worker-5.tyanh185.workers.dev
```

Khi truyền file này qua `--worker-url`, mỗi async worker sẽ bám một Worker URL theo thứ tự để chia tải.

## 3. Chạy Nền Part 1: Qua Cloudflare Workers

```bash
./runners/run_selenium_worker_daemon.sh --name tiki_part1_worker -- \
  --input data/input/product_ids_part1.txt \
  --output-dir data/output/selenium_worker_test/part1_parts \
  --batch-size 1000 \
  --concurrency 5 \
  --delay-min 1.5 \
  --delay-max 4 \
  --worker-url data/input/worker_urls.txt \
  --cookie-file data/session/tiki_browser_session.json
```

## 4. Chạy Nền Part 2: Qua Cloudflare Workers

```bash
./runners/run_selenium_worker_daemon.sh --name tiki_worker -- \
  --input data/input/product_ids_part2.txt \
  --output-dir data/output/selenium_worker_test/parts \
  --batch-size 1000 \
  --concurrency 5 \
  --delay-min 1.5 \
  --delay-max 4 \
  --worker-url data/input/worker_urls.txt \
  --cookie-file data/session/tiki_browser_session.json
```

Không cho 2 process ghi cùng `--output-dir`.

## 5. Dừng Từng Daemon

Dừng part1:

```bash
./runners/stop_selenium_worker_daemon.sh --name tiki_part1_worker
```

Dừng part2:

```bash
./runners/stop_selenium_worker_daemon.sh --name tiki_worker
```

## 6. Kiểm Tra Process Đang Chạy

Kiểm tra PID file:

```bash
ls -l logs/tiki_part1_worker.pid logs/tiki_worker.pid 2>/dev/null
```

Kiểm tra process tổng quát:

```bash
ps aux | grep -E "tiki_part1_worker|tiki_worker|src/main.py|caffeinate" | grep -v grep
```

Kiểm tra từng PID:

```bash
ps -p $(cat logs/tiki_part1_worker.pid) -o pid,ppid,etime,stat,command
ps -p $(cat logs/tiki_worker.pid) -o pid,ppid,etime,stat,command
```

Nếu chỉ thấy `tail -f ...log` thì đó chỉ là cửa sổ xem log, không phải crawler.

## 7. Xem Log

Part1:

```bash
tail -f logs/tiki_part1_worker.log
```

Part2:

```bash
tail -f logs/tiki_worker.log
```

Log chạy đúng Worker mode sẽ có dạng:

```text
🌐 API endpoint: ☁️  Cloudflare Workers (5) → ...
🚀 Khởi chạy 5 worker(s) bất đồng bộ...
```

## 8. Kiểm Tra Output Có Tăng Không

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

## 9. Kiểm Tra WAF / Retry / Cooldown

```bash
./venv/bin/python - <<'PY'
import json, time
from pathlib import Path

for retry_file in sorted(Path("data/output/selenium_worker_test").glob("**/products_part_*.retry.json")):
    try:
        data = json.loads(retry_file.read_text(encoding="utf-8"))
    except Exception:
        continue

    retry_items = [key for key in data if key != "_global"]
    global_state = data.get("_global")
    remaining = 0
    reason = None

    if global_state and global_state.get("next_retry_at"):
        remaining = max(0, int(float(global_state["next_retry_at"]) - time.time() + 0.999))
        reason = global_state.get("reason")

    if retry_items or remaining:
        print(f"{retry_file}: retry_items={len(retry_items)}, global_cooldown={remaining}s, reason={reason}")
PY
```

Nếu `global_cooldown > 0`, daemon sẽ tự chờ rồi resume khi hết cooldown.

## 10. Đếm Tổng Kết Quả

```bash
./venv/bin/python - <<'PY'
import json, time
from pathlib import Path

roots = [
    ("part1_worker", Path("data/output/selenium_worker_test/part1_parts"), 100000),
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

## 11. Resume

Chạy lại đúng lệnh cũ, giữ nguyên:

- `--input`
- `--output-dir`
- `--batch-size`

Crawler sẽ bỏ qua ID đã thành công trong `.progress.txt`, giữ lỗi tạm thời trong `.retry.json`, và tiếp tục các ID đến hạn.

## 12. WAF Backoff

Khi gặp HTML challenge/WAF:

```text
5 phút -> 15 phút -> 30 phút -> 1 tiếng -> tiếp tục 1 tiếng
```

Nếu daemon còn chạy, `main.py` sẽ tự chờ và resume sau cooldown.
