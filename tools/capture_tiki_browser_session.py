#!/usr/bin/env python3
"""
Capture browser cookies/user-agent with Selenium for the Tiki API crawler.

This is intentionally separate from the main crawler. Selenium is used only to
bootstrap a browser-like session; high-volume product fetching remains aiohttp.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any


DEFAULT_OUTPUT = Path("data/session/tiki_browser_session.json")
DEFAULT_PRODUCT_ID = 138083218
TIKI_HOME_URL = "https://tiki.vn/"
TIKI_API_BASE_URL = "https://api.tiki.vn/product-detail/api/v1/products"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture Tiki browser session cookies with Selenium",
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--product-id", type=int, default=DEFAULT_PRODUCT_ID)
    parser.add_argument(
        "--worker-url",
        type=str,
        default=None,
        help="Optional Cloudflare Worker endpoint to probe after visiting Tiki",
    )
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=8.0,
        help="Seconds to wait after each page load",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome headless. Omit this if you need to solve a visual challenge manually.",
    )
    parser.add_argument(
        "--keep-open",
        action="store_true",
        help="Keep Chrome open after capture so you can inspect the final page.",
    )
    return parser.parse_args()


def import_selenium():
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
    except ImportError as exc:
        raise SystemExit(
            "Chưa cài Selenium. Cài bằng:\n"
            "  ./venv/bin/python -m pip install -r requirements-selenium.txt"
        ) from exc
    return webdriver, Options, Service


def build_driver(headless: bool):
    webdriver, Options, Service = import_selenium()
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--window-size=1440,1000")
    options.add_argument("--lang=vi-VN")
    options.add_argument("--accept-lang=vi-VN,vi,en-US,en")
    return webdriver.Chrome(service=Service(), options=options)


def dedupe_cookies(cookies: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[str, str, str], dict[str, Any]] = {}
    for cookie in cookies:
        name = str(cookie.get("name", "")).strip()
        domain = str(cookie.get("domain", "")).strip()
        path = str(cookie.get("path", "/")).strip() or "/"
        value = cookie.get("value")
        if not name or value is None:
            continue
        deduped[(domain, path, name)] = {
            "name": name,
            "value": str(value),
            "domain": domain,
            "path": path,
            "secure": bool(cookie.get("secure", False)),
            "httpOnly": bool(cookie.get("httpOnly", False)),
        }
    return sorted(deduped.values(), key=lambda item: (item["domain"], item["path"], item["name"]))


def visit_and_collect(driver, url: str, wait_seconds: float) -> tuple[list[dict[str, Any]], str]:
    print(f"Open: {url}")
    driver.get(url)
    time.sleep(wait_seconds)
    title = driver.title or ""
    cookies = driver.get_cookies()
    print(f"  title={title!r}, cookies={len(cookies)}")
    return cookies, title


def main() -> None:
    args = parse_args()
    driver = build_driver(args.headless)
    all_cookies: list[dict[str, Any]] = []
    probes: list[dict[str, Any]] = []

    try:
        cookies, title = visit_and_collect(driver, TIKI_HOME_URL, args.wait_seconds)
        all_cookies.extend(cookies)
        probes.append({"url": TIKI_HOME_URL, "title": title, "cookies": len(cookies)})

        api_url = f"{TIKI_API_BASE_URL}/{args.product_id}"
        cookies, title = visit_and_collect(driver, api_url, args.wait_seconds)
        all_cookies.extend(cookies)
        probes.append({"url": api_url, "title": title, "cookies": len(cookies)})

        if args.worker_url:
            worker_url = f"{args.worker_url.rstrip('/')}/{args.product_id}"
            cookies, title = visit_and_collect(driver, worker_url, args.wait_seconds)
            all_cookies.extend(cookies)
            probes.append({"url": worker_url, "title": title, "cookies": len(cookies)})

        user_agent = driver.execute_script("return navigator.userAgent")
        payload = {
            "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "user_agent": user_agent,
            "cookies": dedupe_cookies(all_cookies),
            "probes": probes,
        }

        args.output.parent.mkdir(parents=True, exist_ok=True)
        temp_file = args.output.with_suffix(args.output.suffix + ".tmp")
        temp_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_file.replace(args.output)
        print(f"Saved browser session: {args.output} ({len(payload['cookies'])} cookies)")

        if args.keep_open:
            input("Press Enter to close Chrome...")
    finally:
        if not args.keep_open:
            driver.quit()


if __name__ == "__main__":
    main()
