# Resume: fetch_tiki_products.py

Thoi gian cap nhat: 2026-09-04, Asia/Ho_Chi_Minh.

## Muc tieu

Lay thong tin san pham Tiki tu danh sach `product_id`, luu JSON theo batch, moi file khoang 1000 san pham. Cac field can lay:

- `id`
- `name`
- `url_key`
- `price`
- `description`
- `images_url`

Yeu cau them:

- Chuan hoa noi dung `description`.
- Giam nguy co bi Tiki tra HTML security/rate-limit.
- Co checkpoint/resume de khong mat du lieu khi bi chan giua chung.
- Thu truoc voi `data/input/product_ids_10.txt`, output `data/output/products_ids_10_output.json`.

## Tinh trang script

File chinh: `fetch_tiki_products.py`.

Script da duoc dieu chinh theo huong chay cham va co kha nang resume:

- Gioi han concurrency chi nhan `1` hoac `2`, mac dinh `1`.
- Them delay ngau nhien giua cac request, mac dinh `2-8` giay.
- Dung chung mot `aiohttp.ClientSession` va `TCPConnector`.
- Dung header giong browser, lay tu `config.DEFAULT_HEADERS`.
- Phat hien response HTML/security challenge thay vi coi la JSON hop le.
- Khi gap HTML security challenge:
  - Ghi id vao retry queue.
  - Ghi global cooldown.
  - Dung run hien tai de tranh spam API.
- Luu san pham ngay khi lay thanh cong, khong doi du 1000 san pham.
- Tao cac file phu de resume:
  - `.jsonl`: append tung san pham thanh cong, co fsync.
  - `.progress.txt`: danh sach id da thanh cong.
  - `.retry.json`: retry state va cooldown.
- Khi chay lai, script doc output/spool/progress va bo qua id da thanh cong.
- Backoff retry: `30s -> 120s -> 600s`.
- Loi terminal nhu `404` khong retry.

## Input test

File `data/input/product_ids_10.txt` hien co 10 IDs:

```text
1391347
74897599
154155413
253117062
130978358
214009046
171618108
179970479
139457837
197334787
```

## Lenh da chay test

```bash
./venv/bin/python -u fetch_tiki_products.py \
  --input data/input/product_ids_10.txt \
  --output data/output/products_ids_10_output.json \
  --concurrency 1 \
  --delay-min 2 \
  --delay-max 8
```

## Ket qua test voi 10 san pham

Script chay thanh cong `10/10` san pham, khong gap HTML rate-limit trong lan test nay.

Log chinh:

```text
IDs=10, da thanh cong=0, can xu ly=10, dang backoff=0
[OK] 1391347 -> da luu ngay (1 products)
[OK] 74897599 -> da luu ngay (2 products)
[OK] 154155413 -> da luu ngay (3 products)
[OK] 253117062 -> da luu ngay (4 products)
[OK] 130978358 -> da luu ngay (5 products)
[OK] 214009046 -> da luu ngay (6 products)
[OK] 171618108 -> da luu ngay (7 products)
[OK] 179970479 -> da luu ngay (8 products)
[OK] 139457837 -> da luu ngay (9 products)
[OK] 197334787 -> da luu ngay (10 products)
Ket thuc: moi=10, loi=0, tong da luu=10. Output: data/output/products_ids_10_output.json
```

## File output da tao

- `data/output/products_ids_10_output.json`
- `data/output/products_ids_10_output.jsonl`
- `data/output/products_ids_10_output.progress.txt`
- `data/output/products_ids_10_output.retry.json`

Kiem tra nhanh output:

```json
{
  "count": 10,
  "ids": [
    1391347,
    74897599,
    154155413,
    253117062,
    130978358,
    214009046,
    171618108,
    179970479,
    139457837,
    197334787
  ],
  "sample": {
    "id": 1391347,
    "name": "Tranh xep Hinh Tia Sang  Guong Thu Tia Sang (2035 Manh Ghep)",
    "url_key": "tranh-xep-hinh-tia-sang-guong-thu-tia-sang-2035-manh-ghep-p1391347",
    "price": 245700,
    "description_length": 996,
    "images_url_count": 2
  }
}
```

## Test tu dong

Da chay:

```bash
./venv/bin/python -m unittest discover -s tests -v
```

Ket qua: `6/6` tests pass.

Test bao gom:

- HTML 200 khong bi xem la product JSON hop le.
- HTML 200 co retry roi JSON thanh cong.
- 404 la loi terminal, khong retry.
- Product duoc luu durable va resume duoc.
- Retry schedule va global cooldown duoc persist.
- Terminal failure khong bi dua vao retry.

## Nguyen nhan ket qua hien tai

Lan test 10 san pham thanh cong vi script dang chay voi toc do cham va pattern request it gay nghi ngo hon:

- Chi 1 request dong thoi.
- Delay ngau nhien 2-8 giay.
- Dung chung session/cookie/connection.
- Header day du hon, giong browser.

Tuy nhien khong dam bao 100% tranh duoc WAF cua Tiki khi chay 500, 1000 hoac 200k san pham. Diem quan trong la script hien da co co che dung an toan, retry queue va resume de chay tiep sau cooldown thay vi mat du lieu.

## Cach chay tiep de test 500-1000 san pham

Nen tao file input rieng 500 hoac 1000 IDs, sau do chay:

```bash
./venv/bin/python -u fetch_tiki_products.py \
  --input data/input/product_ids_500.txt \
  --output data/output/products_ids_500_output.json \
  --concurrency 1 \
  --delay-min 2 \
  --delay-max 8
```

Neu gap security challenge, dung retry ngay. Cho qua cooldown roi chay lai dung lenh cu. Script se doc `.progress.txt` va `.retry.json` de chi xu ly phan con thieu.
