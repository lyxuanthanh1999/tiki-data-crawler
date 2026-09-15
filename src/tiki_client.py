"""HTTP client logic cho Tiki product-detail API.

Module này chỉ làm việc với một request sản phẩm:
- gọi endpoint product-detail;
- nhận diện response JSON hợp lệ;
- phân loại lỗi 404, retryable HTTP, HTML WAF challenge, Cloudflare quota;
- chuẩn hóa product field thông qua `cleaner.extract_product_fields`.
"""

import asyncio
import json
import time
from typing import Optional

import aiohttp

from cleaner import extract_product_fields
from config import TIKI_API_BASE_URL
from crawler_models import FetchResult
from pacer import NaturalPacer

REQUIRED_PRODUCT_FIELDS = {"id", "name", "price"}


def parse_retry_after(header_value: Optional[str]) -> Optional[int]:
    """Đọc header Retry-After dạng số giây hoặc HTTP-date."""
    if not header_value:
        return None
    try:
        return max(1, int(header_value.strip()))
    except ValueError:
        try:
            from email.utils import parsedate_to_datetime

            delta = int(parsedate_to_datetime(header_value.strip()).timestamp() - time.time())
            return max(1, delta)
        except Exception:
            return None


def is_cloudflare_quota_response(status: int, body: str, api_base_url: str) -> bool:
    """Nhận diện lỗi quota/rate-limit trả bởi Cloudflare Worker."""
    body_lower = body.lower()
    quota_signal = (
        status in {429, 530, 1015, 1027}
        or "exceeded" in body_lower
        or "worker rate limit" in body_lower
        or "error 1015" in body_lower
        or "error 1027" in body_lower
    )
    worker_signal = (
        "cloudflare" in body_lower
        or "worker" in body_lower
        or "workers.dev" in api_base_url
    )
    return quota_signal and worker_signal


async def fetch_product(
    session: aiohttp.ClientSession,
    pacer: NaturalPacer,
    product_id: int,
    api_base_url: str = TIKI_API_BASE_URL,
) -> FetchResult:
    """
    Gọi API cho một product_id và trả về `FetchResult`.

    Hàm này không tự retry. Nó chỉ phân loại kết quả để `product_runner`
    quyết định lưu success, ghi failed permanent, hay đưa ID vào retry queue.
    """
    await pacer.wait()
    url = f"{api_base_url.rstrip('/')}/{product_id}"
    try:
        async with session.get(url) as response:
            body = await response.text()
            content_type = response.headers.get("Content-Type", "").lower()
            if is_cloudflare_quota_response(response.status, body, api_base_url):
                return FetchResult("quota_exceeded", reason="cf_worker_quota_exceeded")
            if response.status == 404:
                return FetchResult("terminal", reason="http_404")
            if response.status in {403, 429}:
                return FetchResult(
                    "retry",
                    reason=f"http_{response.status}",
                    retry_after=parse_retry_after(response.headers.get("Retry-After")),
                )
            if response.status >= 500:
                return FetchResult(
                    "retry",
                    reason=f"http_{response.status}",
                    retry_after=parse_retry_after(response.headers.get("Retry-After")),
                )
            if response.status != 200:
                return FetchResult("terminal", reason=f"http_{response.status}")
            if "html" in content_type or body.lstrip().startswith("<"):
                return FetchResult("challenge", reason="html_security_challenge")
            if "json" not in content_type:
                return FetchResult("retry", reason="invalid_content_type")

            try:
                data = json.loads(body)
            except json.JSONDecodeError:
                return FetchResult("retry", reason="invalid_json")

            if not isinstance(data, dict) or data.get("id") is None:
                return FetchResult("terminal", reason="invalid_product_payload")

            missing = REQUIRED_PRODUCT_FIELDS - {key for key, value in data.items() if value is not None}
            if missing:
                fields = ",".join(sorted(missing))
                return FetchResult("retry", reason=f"soft_block_missing_fields:{fields}")

            product = extract_product_fields(data)
            product["images_url"] = product.pop("images")
            return FetchResult("success", product=product)
    except asyncio.TimeoutError:
        return FetchResult("retry", reason="timeout")
    except aiohttp.ClientError as exc:
        return FetchResult("retry", reason=type(exc).__name__)
