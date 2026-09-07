# Resume: Cloudflare Worker proxy / worker-url

Thoi gian cap nhat: 2026-09-07, Asia/Ho_Chi_Minh.

## Muc tieu dang ban

Muốn chạy crawler ổn định hơn bằng cách tách 2 process:

- Process 1 gọi Tiki trực tiếp.
- Process 2 gọi qua Cloudflare Worker Edge Proxy để dùng endpoint/IP khác.

Mục tiêu chính là tránh 2 process cùng một IP làm tăng tổng request rate và cùng bị HTML challenge.

## Endpoint Worker dự kiến

```text
https://tiki-proxy-worker.tyanh185.workers.dev
```

Khi dùng Worker, crawler sẽ gọi:

```text
https://tiki-proxy-worker.tyanh185.workers.dev/{product_id}
```

Thay vì gọi trực tiếp:

```text
https://api.tiki.vn/product-detail/api/v1/products/{product_id}
```

## Thay đổi code cần có / đã được mô tả

### fetch_tiki_products.py

Cần hỗ trợ tham số:

```bash
--worker-url
```

Các điểm đổi:

1. `parse_args()`

Thêm:

```python
parser.add_argument(
    "--worker-url",
    type=str,
    default=None,
    help="URL Cloudflare Worker (bỏ trống = gọi trực tiếp Tiki API)",
)
```

2. `fetch_product()`

Đổi signature:

```python
async def fetch_product(session, pacer, product_id, api_base_url: str) -> FetchResult:
```

Đổi URL:

```python
url = f"{api_base_url}/{product_id}"
```

Thay cho hardcode:

```python
url = f"{TIKI_API_BASE_URL}/{product_id}"
```

3. `run_product_ids()`

Resolve endpoint một lần lúc bắt đầu:

```python
worker_url = getattr(args, "worker_url", None)
api_base_url = worker_url.rstrip("/") if worker_url else TIKI_API_BASE_URL
print(
    f"🌐 API endpoint: "
    f"{'☁️  Cloudflare Worker' if worker_url else '🎯 Tiki trực tiếp'} → {api_base_url}"
)
```

4. Truyền endpoint xuống worker:

```python
result = await fetch_product(session, pacer, product_id, api_base_url)
```

5. Chọn headers theo endpoint:

Khi gọi trực tiếp Tiki:

```python
session_headers = DEFAULT_HEADERS
```

Khi gọi qua Worker:

```python
session_headers = {
    "Accept": "application/json",
    "User-Agent": DEFAULT_HEADERS["User-Agent"],
}
```

Rồi dùng:

```python
headers=session_headers
```

trong `aiohttp.ClientSession(...)`.

### main.py

Cần thêm `--worker-url` vào parser và truyền xuống `batch_args`:

```python
parser.add_argument(
    "--worker-url",
    type=str,
    default=None,
    help="URL Cloudflare Worker (bỏ trống = gọi trực tiếp Tiki API)",
)
```

Trong `SimpleNamespace`:

```python
batch_args = SimpleNamespace(
    output=output_file,
    concurrency=args.concurrency,
    delay_min=args.delay_min,
    delay_max=args.delay_max,
    timeout=args.timeout,
    worker_url=args.worker_url,
)
```

Lưu ý: trong phần chat có nhắc "main.py không cần sửa" nếu truyền nguyên args, nhưng code hiện tại tạo `SimpleNamespace` thủ công, nên thực tế `main.py` cần truyền thêm `worker_url`.

## Kế hoạch chạy 2 process

### Bước 1: Chia input thành 2 phần

Tạo:

- `data/input/product_ids_part1.txt`
- `data/input/product_ids_part2.txt`

Lệnh dự kiến:

```bash
python3 -c "
lines = open('data/input/product_ids.txt').read().splitlines()
half = len(lines) // 2
open('data/input/product_ids_part1.txt','w').write('\n'.join(lines[:half]))
open('data/input/product_ids_part2.txt','w').write('\n'.join(lines[half:]))
print(f'Part1: {half} IDs, Part2: {len(lines)-half} IDs')
"
```

### Bước 2: Tạo 2 Run Config trong PyCharm

Process 1 - Tiki Direct:

```text
--input data/input/product_ids_part1.txt --output-dir data/output/concurrency/parts_direct --batch-size 1000 --concurrency 10 --delay-min 0.8 --delay-max 2.0
```

Process 2 - Cloudflare Worker:

```text
--input data/input/product_ids_part2.txt --output-dir data/output/concurrency/parts_worker --batch-size 1000 --concurrency 15 --delay-min 0.8 --delay-max 2.0 --worker-url https://tiki-proxy-worker.tyanh185.workers.dev
```

Quan trọng: dùng output dir khác nhau cho 2 process để tránh race/corrupt file.

## Log kỳ vọng

Process 1:

```text
🌐 API endpoint: 🎯 Tiki trực tiếp → https://api.tiki.vn/product-detail/api/v1/products
🚀 Khởi chạy 10 worker(s) bất đồng bộ...
```

Process 2:

```text
🌐 API endpoint: ☁️  Cloudflare Worker → https://tiki-proxy-worker.tyanh185.workers.dev
🚀 Khởi chạy 15 worker(s) bất đồng bộ...
```

## Rủi ro / lưu ý

- Cloudflare Worker có thể giúp đổi endpoint/IP outbound, nhưng không đảm bảo tránh WAF.
- Tiki có thể vẫn nhìn fingerprint/pattern/rate và trả HTML challenge.
- Không nên chạy cả 2 process vào cùng `data/output/concurrency/parts`.
- Nếu Worker bị challenge nhanh, giảm `concurrency` hoặc tăng delay.
- Nếu chạy 2 process cùng máy không qua Worker/VPS/proxy, tổng request rate cùng IP tăng và dễ làm cả hai process dính HTML challenge.

## Việc tiếp theo

1. Kiểm tra thực tế code hiện tại đã có `--worker-url` chưa.
2. Nếu chưa có, patch `fetch_tiki_products.py` và `main.py` theo các điểm trên.
3. Chạy `py_compile` và unit tests.
4. Tạo file input part1/part2.
5. Test Worker với `--limit 1000` trước khi chạy dài.
