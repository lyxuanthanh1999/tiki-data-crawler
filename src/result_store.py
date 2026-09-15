"""Durable output, progress, retry, and stats storage.

Module này là nền tảng resume. Mỗi output batch có một bộ file đi kèm:
- `.jsonl` để append từng product ngay khi thành công;
- `.progress.txt` để biết ID nào đã lưu;
- `.retry.json` để giữ ID lỗi tạm thời và cooldown WAF;
- `.failed_permanent.json` cho lỗi terminal như 404;
- `.stats.json` cho thời gian chạy tích lũy.
"""

import asyncio
import json
import os
import time
from pathlib import Path
from typing import Any, Optional

from time_utils import format_duration_clock, format_duration_human

# Retryable failures keep cycling through these backoff windows.
BACKOFF_SECONDS = (300, 900, 1800, 3600)
WAF_BACKOFF_SECONDS = (300, 900, 1800, 3600)


class ResultStore:
    """
    Store crawler state beside each output JSON file.

    Files written:
    - .jsonl: append-only product spool, safest source for resume;
    - .progress.txt: successful IDs;
    - .retry.json: transient retry queue and global WAF cooldown;
    - .failed_permanent.json: terminal IDs such as 404;
    - .stats.json: accumulated runtime statistics.
    """

    def __init__(self, output_file: Path):
        self.output_file = output_file
        self.spool_file = output_file.with_suffix(".jsonl")
        self.progress_file = output_file.with_suffix(".progress.txt")
        self.retry_file = output_file.with_suffix(".retry.json")
        self.stats_file = output_file.with_suffix(".stats.json")
        self.failed_permanent_file = output_file.with_suffix(".failed_permanent.json")
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        self.products: list[dict[str, Any]] = []
        self.successful_ids: set[int] = set()
        self.retry_state: dict[str, dict[str, Any]] = {}
        self.failed_permanent: dict[str, dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        """Load all persisted state so a new run can resume safely."""
        source = self.spool_file if self.spool_file.exists() else self.output_file
        if source.exists():
            try:
                if source.suffix == ".jsonl":
                    with source.open("r", encoding="utf-8") as file:
                        products = [json.loads(line) for line in file if line.strip()]
                else:
                    with source.open("r", encoding="utf-8") as file:
                        products = json.load(file)
                for product in products:
                    product_id = product.get("id")
                    if product_id is not None and int(product_id) not in self.successful_ids:
                        self.products.append(product)
                        self.successful_ids.add(int(product_id))
            except (OSError, json.JSONDecodeError, TypeError, ValueError) as exc:
                raise RuntimeError(f"Không đọc được dữ liệu đã lưu: {source}: {exc}") from exc

        if self.progress_file.exists():
            with self.progress_file.open("r", encoding="utf-8") as file:
                self.successful_ids.update(
                    int(line.strip()) for line in file if line.strip().isdigit()
                )

        self.retry_state = self._load_json_dict(self.retry_file)
        self.failed_permanent = self._load_json_dict(self.failed_permanent_file)

    @staticmethod
    def _load_json_dict(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            with path.open("r", encoding="utf-8") as file:
                data = json.load(file)
        except (OSError, json.JSONDecodeError):
            return {}
        return data if isinstance(data, dict) else {}

    @staticmethod
    def _append_durable(path: Path, text: str) -> None:
        """Append and fsync immediately so interruption does not lose progress."""
        with path.open("a", encoding="utf-8") as file:
            file.write(text)
            file.flush()
            os.fsync(file.fileno())

    @staticmethod
    def _write_json_atomic(path: Path, data: Any) -> None:
        """Write JSON through a temp file then replace the final path atomically."""
        temp_file = path.with_suffix(path.suffix + ".tmp")
        with temp_file.open("w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        temp_file.replace(path)

    def update_stats(self, last_run_duration: float) -> dict[str, Any]:
        """Cộng dồn thời gian chạy vào `.stats.json` sau mỗi lượt crawl batch."""
        previous_stats = self._load_json_dict(self.stats_file)
        previous_total = float(previous_stats.get("total_time_seconds", 0) or 0)
        total_time = previous_total + max(0, last_run_duration)
        total_saved = len(self.products)
        average_seconds = total_time / total_saved if total_saved else None
        stats = {
            "total_products_saved": total_saved,
            "total_time_seconds": round(total_time, 1),
            "total_time_formatted": format_duration_clock(total_time),
            "total_time_human": format_duration_human(total_time),
            "average_seconds_per_product": (
                round(average_seconds, 2) if average_seconds is not None else None
            ),
            "average_speed": (
                f"{average_seconds:.2f}s / sản phẩm"
                if average_seconds is not None
                else "N/A"
            ),
            "last_run_duration_seconds": round(max(0, last_run_duration), 1),
            "last_run_duration": f"{max(0, last_run_duration):.1f}s",
            "updated_at": time.strftime("%Y-%m-%d %H:%M:%S %z"),
        }
        self._write_json_atomic(self.stats_file, stats)
        return stats

    async def save_product(self, product: dict[str, Any]) -> bool:
        """Persist one successful product and clear stale retry/failure state."""
        product_id = int(product["id"])
        async with self._lock:
            if product_id in self.successful_ids:
                return False
            self._append_durable(
                self.spool_file,
                json.dumps(product, ensure_ascii=False) + "\n",
            )
            self._append_durable(self.progress_file, f"{product_id}\n")
            self.products.append(product)
            self.successful_ids.add(product_id)
            self.retry_state.pop(str(product_id), None)
            self.failed_permanent.pop(str(product_id), None)
            self._write_json_atomic(self.output_file, self.products)
            self._write_json_atomic(self.retry_file, self.retry_state)
            return True

    async def record_failure(
        self,
        product_id: int,
        reason: str,
        retryable: bool = True,
        stop_all: bool = False,
        server_retry_after: Optional[int] = None,
    ) -> int:
        """Record a terminal failure or schedule a retry/backoff."""
        async with self._lock:
            key = str(product_id)
            attempts = int(self.retry_state.get(key, {}).get("attempts", 0)) + 1
            wait_seconds, next_retry_at = self._next_retry(retryable, stop_all, attempts, server_retry_after)

            self.retry_state[key] = {
                "attempts": attempts,
                "reason": reason,
                "next_retry_at": next_retry_at,
            }

            if stop_all:
                self.retry_state["_global"] = {
                    "attempts": int(self.retry_state.get("_global", {}).get("attempts", 0)) + 1,
                    "reason": reason,
                    "next_retry_at": next_retry_at,
                }

            if next_retry_at is None:
                self.failed_permanent[key] = {
                    "reason": reason,
                    "attempts": attempts,
                    "timestamp": time.time(),
                }
                self.retry_state.pop(key, None)
                self._write_json_atomic(self.failed_permanent_file, self.failed_permanent)

            self._write_json_atomic(self.retry_file, self.retry_state)
            return wait_seconds

    def _next_retry(
        self,
        retryable: bool,
        stop_all: bool,
        attempts: int,
        server_retry_after: Optional[int],
    ) -> tuple[int, Optional[float]]:
        """
        Tính lần retry tiếp theo.

        - Lỗi terminal: không retry.
        - WAF/challenge: dùng backoff toàn cục.
        - HTTP retryable có Retry-After: ưu tiên header server.
        - Retryable thường: dùng chuỗi 5p, 15p, 30p, 1h.
        """
        if not retryable:
            return 0, None
        if stop_all:
            global_attempts = int(self.retry_state.get("_global", {}).get("attempts", 0)) + 1
            wait_seconds = WAF_BACKOFF_SECONDS[
                min(global_attempts - 1, len(WAF_BACKOFF_SECONDS) - 1)
            ]
        elif server_retry_after is not None:
            wait_seconds = server_retry_after
        else:
            wait_seconds = BACKOFF_SECONDS[min(attempts - 1, len(BACKOFF_SECONDS) - 1)]
        return wait_seconds, time.time() + wait_seconds

    def is_due(self, product_id: int) -> bool:
        """Return True when an ID is not done and is ready to request now."""
        if str(product_id) in self.failed_permanent:
            return False
        state = self.retry_state.get(str(product_id))
        if not state:
            return True
        next_retry_at = state.get("next_retry_at")
        return next_retry_at is not None and time.time() >= float(next_retry_at)

    def global_wait_remaining(self) -> int:
        """Return remaining WAF cooldown seconds, clearing expired global state."""
        state = self.retry_state.get("_global")
        if not state:
            return 0
        remaining = float(state.get("next_retry_at", 0)) - time.time()
        if remaining <= 0:
            self.retry_state.pop("_global", None)
            self._write_json_atomic(self.retry_file, self.retry_state)
            return 0
        return int(remaining + 0.999)

    @property
    def permanently_failed_count(self) -> int:
        """Số ID terminal đã ghi vào `.failed_permanent.json`."""
        return len(self.failed_permanent)
