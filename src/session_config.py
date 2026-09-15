"""Load browser session and Cloudflare Worker endpoint configuration.

Selenium chỉ dùng để capture cookie/user-agent một lần. Crawler chính đọc file
session này và gắn vào request `aiohttp`. Worker URL cũng được đọc tại đây để
`product_runner` phân bổ endpoint cho từng async worker.
"""

import json
from pathlib import Path
from typing import Optional

from config import DEFAULT_HEADERS


def load_worker_urls(worker_url: Optional[str]) -> list[str]:
    """
    Resolve Cloudflare Worker endpoints from CLI input.

    Supported forms:
    - one URL;
    - comma/newline separated URLs;
    - a .txt file containing one URL per line.
    """
    if not worker_url:
        return []

    candidate = Path(worker_url)
    raw_text = candidate.read_text(encoding="utf-8") if candidate.exists() else worker_url

    urls: list[str] = []
    seen: set[str] = set()
    for part in raw_text.replace(",", "\n").splitlines():
        url = part.strip().rstrip("/")
        if not url or url.startswith("#") or url in seen:
            continue
        urls.append(url)
        seen.add(url)
    return urls


def load_browser_session(cookie_file: Optional[Path]) -> tuple[dict[str, str], int]:
    """Convert a Selenium-captured session JSON file into request headers."""
    if not cookie_file:
        return {}, 0
    if not cookie_file.exists():
        raise FileNotFoundError(f"Không tìm thấy cookie file: {cookie_file}")

    data = json.loads(cookie_file.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Cookie file không đúng định dạng JSON object: {cookie_file}")

    headers: dict[str, str] = {}
    user_agent = data.get("user_agent")
    if isinstance(user_agent, str) and user_agent.strip():
        headers["User-Agent"] = user_agent.strip()

    cookies = data.get("cookies", [])
    cookie_pairs: list[str] = []
    if isinstance(cookies, list):
        seen: set[str] = set()
        for cookie in cookies:
            if not isinstance(cookie, dict):
                continue
            name = cookie.get("name")
            value = cookie.get("value")
            if not isinstance(name, str) or not isinstance(value, str):
                continue
            if not name or name in seen:
                continue
            seen.add(name)
            cookie_pairs.append(f"{name}={value}")

    if cookie_pairs:
        headers["Cookie"] = "; ".join(cookie_pairs)
    return headers, len(cookie_pairs)


def build_session_headers(is_worker_mode: bool, cookie_file: Optional[Path]) -> tuple[dict[str, str], int]:
    """
    Tạo header cho aiohttp session.

    Worker mode dùng header gọn hơn vì request đi qua proxy. Direct mode giữ
    bộ header browser-like đầy đủ từ `config.DEFAULT_HEADERS`.
    """
    if is_worker_mode:
        headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": DEFAULT_HEADERS["User-Agent"],
        }
    else:
        headers = DEFAULT_HEADERS.copy()

    session_headers, cookie_count = load_browser_session(cookie_file)
    headers.update(session_headers)
    return headers, cookie_count
