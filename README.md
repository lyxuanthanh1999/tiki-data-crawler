# Data Collection

Công cụ thu thập thông tin sản phẩm từ Tiki Product Detail API và lưu kết quả thành các file JSON theo batch. Số lượng sản phẩm không bị giới hạn bởi hệ thống.

## Dữ liệu

Input mặc định:

```text
data/input/product_ids.txt
```

Danh sách input có thể có bất kỳ số lượng product ID. Mỗi batch mặc định có 1.000 ID. Kết quả được lưu tại:

```text
data/output/concurrency/parts/
```

Một batch gồm các file checkpoint:

- `products_part_0001.json`: dữ liệu sản phẩm đã chuẩn hóa.
- `products_part_0001.jsonl`: dữ liệu append để khôi phục khi bị gián đoạn.
- `products_part_0001.progress.txt`: ID đã lấy thành công.
- `products_part_0001.retry.json`: ID cần retry và thời điểm retry.
- `products_part_0001.failed_permanent.json`: lỗi không retry tiếp, ví dụ 404.
- `products_part_0001.stats.json`: thống kê thời gian chạy.

## Cấu trúc chính

```text
src/
  main.py                 Điểm vào chạy theo batch.
  cli.py                  Khai báo và kiểm tra command-line arguments.
  batch_runner.py         Điều phối batch, retry, resume và WAF cooldown.
  batch_storage.py        Đọc ID, chia batch, đọc và seed checkpoint.
  cooldown.py             Chờ khi API trả về WAF/HTML challenge.
  fetch_tiki_products.py  Engine asyncio/aiohttp gọi Product Detail API.
  cleaner.py              Chuẩn hóa description và các trường sản phẩm.
  config.py               Cấu hình API, header và giá trị mặc định.
```

## Luồng hệ thống

```text
File product IDs
      |
      v
main.py đọc và loại ID trùng
      |
      v
Chia thành các batch
      |
      v
fetch_tiki_products.py
  nhiều worker async
      |
      +---- Thành công ------> JSON và checkpoint
      |
      +---- Lỗi tạm thời ----> retry queue, chạy lại khi đến hạn
      |
      +---- HTML challenge ---> WAF cooldown, tự chờ rồi tiếp tục
      |
      +---- 404/lỗi vĩnh viễn -> failed_permanent.json
```

Mỗi batch chạy xong mới chuyển sang batch kế tiếp. Khi dừng chương trình, có thể chạy lại cùng command để tiếp tục từ checkpoint.

## Cài đặt

Có thể dùng `uv`, một package manager và project manager cho Python:

Tài liệu: https://docs.astral.sh/uv/

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```

Chạy bằng môi trường do `uv` quản lý:

```bash
PYTHONPATH=src uv run python src/main.py --help
```

Hoặc dùng virtualenv và pip:

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Xem command

Chạy từ thư mục gốc:

```bash
PYTHONPATH=src uv run python src/main.py --help
```

## Chạy thử

```bash
PYTHONPATH=src uv run python src/main.py \
  --input data/input/product_ids_10.txt \
  --limit 10 \
  --batch-size 10 \
  --concurrency 2 \
  --delay-min 1 \
  --delay-max 2
```

## Chạy thu thập dữ liệu

```bash
PYTHONPATH=src uv run python src/main.py \
  --input data/input/product_ids.txt \
  --output-dir data/output/concurrency/parts \
  --batch-size 1000 \
  --concurrency 10 \
  --delay-min 0.8 \
  --delay-max 2.0
```

`main.py` chạy từng batch theo thứ tự. Khi một batch hoàn tất, batch tiếp theo sẽ được chạy. Có thể giới hạn phạm vi bằng `--start-batch` và `--end-batch` nếu cần chia phạm vi xử lý.

Ví dụ chạy một phạm vi batch:

```bash
PYTHONPATH=src uv run python src/main.py \
  --input data/input/product_ids.txt \
  --output-dir data/output/concurrency/parts \
  --batch-size 1000 \
  --start-batch 1 \
  --end-batch 20 \
  --concurrency 10 \
  --delay-min 0.8 \
  --delay-max 2.0
```

## Resume và xử lý lỗi

Chạy lại đúng command cũ để resume. Engine tự bỏ qua ID đã lưu thành công, giữ lại retry pending và tiếp tục ID còn thiếu.

Khi gặp HTML challenge, trạng thái cooldown được ghi trong file `*.retry.json`. Mặc định chương trình tự chờ rồi chạy tiếp. Dùng `--no-auto-wait` nếu muốn dừng ngay khi gặp WAF.

Nếu trước đây đã chạy engine trực tiếp và có output tổng hợp, dùng:

```bash
PYTHONPATH=src uv run python src/main.py \
  --input data/input/product_ids.txt \
  --seed-from-output data/output/concurrency/products_output.json \
  --seed-only
```

Sau đó chạy lại command thu thập dữ liệu để tiếp tục các batch còn thiếu.

## Chạy engine trực tiếp

Không chia batch, chỉ dùng khi kiểm thử hoặc xử lý một nhóm ID nhỏ:

```bash
PYTHONPATH=src uv run python src/fetch_tiki_products.py \
  --input data/input/product_ids_10.txt \
  --output data/output/concurrency/products_output.json \
  --concurrency 2 \
  --delay-min 1 \
  --delay-max 2
```

## Kiểm tra

```bash
PYTHONPATH=src uv run python -m unittest discover -s tests
```

Các log runtime nằm trong `logs/` và không được commit vào repository.
