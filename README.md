# Tiki Selenium Worker Hybrid

Nhanh gọn: nhánh `selenium-worker-hybrid` dùng để crawl sản phẩm Tiki bằng API, có hỗ trợ cookie lấy từ Selenium và proxy qua Cloudflare Workers.

Selenium chỉ dùng để lấy browser session/cookie. Phần crawl chính vẫn chạy bằng `aiohttp` để nhanh hơn và có checkpoint/resume.

## Cấu Trúc Chính

```text
src/main.py                  Chia batch 1000 ID và điều phối crawl
src/product_runner.py        Chạy async workers cho từng batch
src/tiki_client.py           Gọi product-detail API, nhận diện WAF/lỗi
src/result_store.py          Lưu json/jsonl/progress/retry/stats
src/session_config.py        Đọc cookie Selenium và Worker URLs
src/cleaner.py               Chuẩn hóa description và field output

runners/run_selenium_worker_daemon.sh   Chạy nền, auto resume
runners/stop_selenium_worker_daemon.sh  Dừng process chạy nền
tools/capture_tiki_browser_session.py   Lấy cookie/user-agent bằng Selenium
tools/merge_parts.py                    Gộp file parts khi cần
```

Output và log hiện dùng:

```text
data/output/selenium_worker_test/part1_parts
data/output/selenium_worker_test/parts
logs/tiki_part1_worker.log
logs/tiki_worker.log
```

## Cài Đặt

Dùng Poetry:

```bash
python3 -m pip install poetry
poetry config virtualenvs.in-project true
poetry install
```

Hoặc dùng `venv` cũ:

```bash
./venv/bin/python -m pip install -r requirements.txt
./venv/bin/python -m pip install -r requirements-selenium.txt
```

Runner sẽ tự ưu tiên `.venv/bin/python`, nếu không có thì dùng `venv/bin/python`, cuối cùng là `python3`.

## Lấy Cookie Selenium

Chạy một lần trước khi crawl:

```bash
poetry run python tools/capture_tiki_browser_session.py \
  --output data/session/tiki_browser_session.json \
  --worker-url https://tiki-proxy-worker.tyanh185.workers.dev \
  --wait-seconds 8
```

Nếu dùng `venv`:

```bash
./venv/bin/python tools/capture_tiki_browser_session.py \
  --output data/session/tiki_browser_session.json \
  --worker-url https://tiki-proxy-worker.tyanh185.workers.dev \
  --wait-seconds 8
```

File cookie tạo ra:

```text
data/session/tiki_browser_session.json
```

## Chạy Crawl Nền

Part 1:

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

Part 2:

```bash
./runners/run_selenium_worker_daemon.sh --name tiki_worker -- \
  --input data/input/product_ids_part2.txt \
  --output-dir data/output/selenium_worker_test/parts \
  --batch-size 1000 \
  --concurrency 10 \
  --delay-min 1.5 \
  --delay-max 4 \
  --worker-url data/input/worker_urls.txt \
  --cookie-file data/session/tiki_browser_session.json
```

Lưu ý:

- `--batch-size 1000` nghĩa là mỗi file part xử lý khoảng 1000 product ID.
- Không cho 2 process dùng cùng `--output-dir`.
- `--worker-url data/input/worker_urls.txt` nghĩa là dùng danh sách nhiều Cloudflare Worker URL trong file đó.
- Nếu dừng rồi chạy lại đúng lệnh cũ, crawler sẽ resume theo `*.jsonl`, `*.progress.txt`, `*.retry.json`.

## Kiểm Tra Đang Chạy

Xem PID file:

```bash
ls -l logs/tiki_part1_worker.pid logs/tiki_worker.pid
cat logs/tiki_part1_worker.pid
cat logs/tiki_worker.pid
```

Kiểm tra process:

```bash
ps aux | grep -E "tiki_part1_worker|tiki_worker|src/main.py|caffeinate" | grep -v grep
```

Kiểm tra theo PID:

```bash
ps -p $(cat logs/tiki_part1_worker.pid) -o pid,ppid,etime,stat,command
ps -p $(cat logs/tiki_worker.pid) -o pid,ppid,etime,stat,command
```

## Xem Log

Theo dõi part 1:

```bash
tail -n 100 -f logs/tiki_part1_worker.log
```

Theo dõi part 2:

```bash
tail -n 100 -f logs/tiki_worker.log
```

Xem nhanh dòng cuối:

```bash
tail -n 60 logs/tiki_part1_worker.log
tail -n 60 logs/tiki_worker.log
```

Log chạy đúng Worker mode sẽ có dạng:

```text
API endpoint: Cloudflare Workers
Khoi chay ... worker(s)
```

## Kiểm Tra Output

Đếm số dòng JSONL đã lưu:

```bash
wc -l data/output/selenium_worker_test/part1_parts/*.jsonl
wc -l data/output/selenium_worker_test/parts/*.jsonl
```

Xem file mới cập nhật gần nhất:

```bash
ls -lt data/output/selenium_worker_test/part1_parts | head
ls -lt data/output/selenium_worker_test/parts | head
```

Đếm tổng kết quả đã lưu:

```bash
find data/output/selenium_worker_test -name "*.jsonl" -print0 | xargs -0 wc -l
```

## Kiểm Tra WAF / Retry

```bash
find data/output/selenium_worker_test -name "*.retry.json" -size +2c -print
```

Nếu log có WAF/HTML challenge, daemon sẽ tự chờ rồi chạy lại. Backoff hiện dùng:

```text
5 phút -> 15 phút -> 30 phút -> 1 tiếng
```

## Dừng Crawl

Dừng part 1:

```bash
./runners/stop_selenium_worker_daemon.sh --name tiki_part1_worker
```

Dừng part 2:

```bash
./runners/stop_selenium_worker_daemon.sh --name tiki_worker
```

Kiểm tra lại sau khi dừng:

```bash
ls logs/*.pid
ps aux | grep -E "tiki_part1_worker|tiki_worker|src/main.py|caffeinate" | grep -v grep
```

## Resume

Muốn chạy tiếp, dùng lại đúng lệnh ở mục "Chạy Crawl Nền".

Không xoá các file này nếu muốn resume:

```text
*.jsonl
*.progress.txt
*.retry.json
*.failed_permanent.json
*.stats.json
```

## Git

Không push các dữ liệu runtime:

```text
data/output/
data/session/
logs/
```
