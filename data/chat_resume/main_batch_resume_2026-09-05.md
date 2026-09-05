# Resume: main.py batch orchestrator

Thoi gian cap nhat: 2026-09-05, Asia/Ho_Chi_Minh.

## Trang thai moi nhat

Da update `main.py` de chay 200k product IDs theo batch, moi batch mac dinh 1000 ID.

`main.py` hien khong dung `crawler.py` nua. Thay vao do, no chia batch va goi lai engine on dinh trong `fetch_tiki_products.py` thong qua `run_product_ids()`.

## Files da update

- `fetch_tiki_products.py`
  - Them `run_product_ids(product_ids, args)` de `main.py` co the goi truc tiep theo batch.
  - Cho phep `--concurrency` tu 1 den 10.
  - Mac dinh van la 5 worker.
  - Da co `.stats.json` tich luy thoi gian qua nhieu lan resume.

- `main.py`
  - Chuyen thanh batch orchestrator.
  - Mac dinh output parts: `data/output/concurrency/parts`.
  - Moi batch tao output rieng: `products_part_0001.json`, `products_part_0002.json`, ...
  - Moi batch co cac file sidecar rieng:
    - `.jsonl`
    - `.progress.txt`
    - `.retry.json`
    - `.stats.json`
    - `.failed_permanent.json`
  - Them `--seed-from-output` de import du lieu da crawl tu output cu.
  - Them `--seed-only` de chi migrate du lieu cu sang parts, khong goi API.
  - Them `--no-seed-existing` neu muon bo qua buoc seed.
  - Them log batch truoc/sau khi chay.
  - Neu batch bi HTML challenge/cooldown thi dung main, chay lai sau cooldown de resume.

- `tests/test_main_batching.py`
  - Them test chia batch.
  - Them test seed output cu sang parts.
  - Them test summarize status batch.

- `README.md`
  - Cap nhat huong dan dung `main.py` voi batch parts.
  - Cap nhat lenh seed-only va lenh chay tiep.

## Trang thai du lieu da seed

Nguon cu:

- `data/output/concurrency/products_output.json`
- `data/output/concurrency/products_output.jsonl`
- `data/output/concurrency/products_output.progress.txt`
- `data/output/concurrency/products_output.failed_permanent.json`

Da chay seed-only:

```bash
./venv/bin/python -u main.py \
  --input data/input/product_ids.txt \
  --output-dir data/output/concurrency/parts \
  --batch-size 1000 \
  --concurrency 5 \
  --delay-min 2 \
  --delay-max 5 \
  --seed-only
```

Ket qua:

```text
Seed tu output cu: them 734 products, 395 permanent fails vao 2 batch(es).
Hoan tat seed-only, khong goi API.
```

Ly do khong phai them du 744/398: thu muc `parts` da co san mot it du lieu tu lan test truoc, nen seed chi them nhung ID chua co.

Trang thai sau seed:

```text
products_part_0001.json: 689 products
products_part_0001.failed_permanent.json: 311 permanent fails
=> batch 0001 da du 1000 trang thai, main.py se bo qua batch nay

products_part_0002.json: 55 products
products_part_0002.failed_permanent.json: 87 permanent fails
=> batch 0002 con thieu, main.py se chay tiep tu batch nay
```

## Log batch moi

Truoc khi chay moi batch, `main.py` se log:

```text
Batch #0001/0001 status truoc chay:
total=1000, success=689, permanent=311, done=1000,
due=0, pending_retry=0, cooldown=0s
```

Neu batch da xong:

```text
Batch #0001 khong co ID den han xu ly. Bo qua.
done=1000/1000, pending_retry=0, output=...
```

Neu batch dang chay:

```text
Batch #0002 dang hoat dong: xu ly 858 ID den han -> data/output/concurrency/parts/products_part_0002.json
```

Neu batch bi HTML challenge/cooldown:

```text
Batch #0002 pending do HTML challenge/cooldown. Chay lai sau it nhat 3600s.
```

Sau khi chay batch:

```text
Batch #0002/0200 status sau chay:
success=..., permanent=..., done=.../1000,
due=..., pending_retry=..., cooldown=...s
```

## Cach chay tiep

Chay tiep 200k theo batch, dung 10 worker:

```bash
./venv/bin/python -u main.py \
  --input data/input/product_ids.txt \
  --output-dir data/output/concurrency/parts \
  --batch-size 1000 \
  --concurrency 10 \
  --delay-min 2 \
  --delay-max 5
```

Giai thich:

- Batch chay tuan tu: batch 0001 xong/skip roi moi toi batch 0002.
- Trong moi batch, `--concurrency 10` tao 10 worker async.
- `NaturalPacer` van gian cach thoi diem bat dau request bang delay random 2-5s, nen khong ban 10 request cung luc ngay lap tuc.
- Neu gap HTML challenge, main se dung va ghi cooldown trong file `.retry.json` cua batch do.
- Chay lai dung lenh cu sau cooldown de resume.

## Lenh kiem tra da chay

```bash
./venv/bin/python -m py_compile main.py fetch_tiki_products.py fetch_tiki_products_sequential.py
./venv/bin/python -m unittest discover -s tests -v
```

Ket qua moi nhat: `11/11` tests pass.
