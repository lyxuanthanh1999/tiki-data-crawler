"""Crawler nhỏ, chạy chậm và có khả năng tiếp tục sau khi bị rate-limit (BytePlus/WAF challenge)."""

import argparse
import asyncio
import json
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import aiohttp

from cleaner import extract_product_fields
from config import DEFAULT_HEADERS, TIKI_API_BASE_URL

# Thời gian chờ backoff (giây) theo cấp số nhân khi retry: lần 1 (30s), lần 2 (120s), lần 3 (600s)
BACKOFF_SECONDS = (30, 120, 600)
# Cooldown tối thiểu (giây) khi gặp WAF challenge mà đã hết lượt backoff.
# Tránh bug vô hạn: script lao vào chạy ngay sau khi challenge vì next_retry_at = None.
CHALLENGE_MIN_COOLDOWN = 3600
# Các field bắt buộc trong raw API response để coi là sản phẩm hợp lệ (phát hiện "soft block").
# LƯU Ý: chỉ dùng field có trong response gốc của API, KHÔNG dùng field được tạo sau khi xử lý
# (vd: images_url được đổi tên từ 'images', description có thể rỗng hợp lệ).
REQUIRED_PRODUCT_FIELDS = {"id", "name", "price"}

DEFAULT_INPUT = Path("data/input/product_ids.txt")
DEFAULT_OUTPUT = Path("data/output/concurrency/products_output.json")


def format_duration_clock(seconds: float) -> str:
    total_seconds = int(round(max(0, seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def format_duration_human(seconds: float) -> str:
    total_seconds = int(round(max(0, seconds)))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: list[str] = []
    if hours:
        parts.append(f"{hours} giờ")
    if minutes:
        parts.append(f"{minutes} phút")
    if secs or not parts:
        parts.append(f"{secs} giây")
    return " ".join(parts)


@dataclass(frozen=True)
class FetchResult:
    """
    Data model đóng gói kết quả sau khi thực hiện 1 request lấy dữ liệu sản phẩm.

    Thuộc tính:
    - status: Trạng thái phản hồi ('success', 'retry', 'challenge', 'terminal').
    - product: Dữ liệu sản phẩm đã chuẩn hóa (nếu thành công).
    - reason: Lý do lỗi hoặc mã lỗi (vd: 'http_404', 'html_security_challenge', 'timeout'...).
    - retry_after: Số giây server yêu cầu chờ (từ header Retry-After), nếu có.
    """
    status: str
    product: Optional[dict[str, Any]] = None
    reason: str = ""
    retry_after: Optional[int] = None  # Fix #2: truyền giá trị Retry-After header từ server


class NaturalPacer:
    """
    Bộ điều tiết nhịp độ (Pacer) nhằm tạo khoảng nghỉ ngẫu nhiên giữa các request.
    Giúp request trông giống hành vi duyệt web tự nhiên của người dùng, giảm nguy cơ bị WAF chặn.
    """

    def __init__(self, delay_min: float, delay_max: float):
        """
        Khởi tạo pacer với ngưỡng delay tối thiểu và tối đa.

        :param delay_min: Thời gian nghỉ tối thiểu (giây).
        :param delay_max: Thời gian nghỉ tối đa (giây).
        """
        self.delay_min = delay_min
        self.delay_max = delay_max
        self._lock = asyncio.Lock()
        self._first_request = True

    async def wait(self) -> None:
        """
        Chờ một khoảng thời gian ngẫu nhiên trong khoảng [delay_min, delay_max].
        Bỏ qua lần chờ đầu tiên để request khởi chạy ngay.
        Dùng asyncio.Lock để đảm bảo các worker luân phiên nhau giãn cách request.
        """
        async with self._lock:
            if self._first_request:
                self._first_request = False
                return
            await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))


class ResultStore:
    """
    Lớp quản lý lưu trữ dữ liệu an toàn và cơ chế checkpoint/resume:
    1. Append ngay từng sản phẩm vào file .jsonl và .progress.txt để chống mất dữ liệu khi crash.
    2. Cập nhật file .json tổng hợp (atomic write) để thuận tiện sử dụng.
    3. Quản lý trạng thái retry (.retry.json) cho từng product_id.
    4. Ghi các ID lỗi vĩnh viễn vào file .failed_permanent.json (Fix #5).
    """

    def __init__(self, output_file: Path):
        """
        Khởi tạo kho lưu trữ dựa trên đường dẫn file output.

        :param output_file: Đường dẫn file JSON đích (vd: data/output/products_output.json).
        """
        self.output_file = output_file
        self.spool_file = output_file.with_suffix(".jsonl")
        self.progress_file = output_file.with_suffix(".progress.txt")
        self.retry_file = output_file.with_suffix(".retry.json")
        self.stats_file = output_file.with_suffix(".stats.json")
        # Fix #5: file riêng cho các ID lỗi vĩnh viễn (terminal hoặc hết backoff)
        self.failed_permanent_file = output_file.with_suffix(".failed_permanent.json")
        self.output_file.parent.mkdir(parents=True, exist_ok=True)
        self.products: list[dict[str, Any]] = []
        self.successful_ids: set[int] = set()
        self.retry_state: dict[str, dict[str, Any]] = {}
        self.failed_permanent: dict[str, dict[str, Any]] = {}  # Fix #5: dict id -> {reason, timestamp}
        # Fix #4: bọc record_failure bằng lock để an toàn khi thêm await trong tương lai
        self._lock = asyncio.Lock()
        self._load()

    def _load(self) -> None:
        """
        Đọc lại toàn bộ tiến độ đã lưu trước đó từ đĩa:
        - Đọc các sản phẩm đã cào thành công từ file .jsonl hoặc .json.
        - Đọc danh sách ID hoàn tất từ file .progress.txt.
        - Đọc trạng thái hàng đợi retry từ file .retry.json.
        - Đọc danh sách ID lỗi vĩnh viễn từ file .failed_permanent.json.
        """
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

        if self.retry_file.exists():
            try:
                with self.retry_file.open("r", encoding="utf-8") as file:
                    self.retry_state = json.load(file)
            except (OSError, json.JSONDecodeError):
                self.retry_state = {}

        # Fix #5: load danh sách lỗi vĩnh viễn đã ghi trước đó
        if self.failed_permanent_file.exists():
            try:
                with self.failed_permanent_file.open("r", encoding="utf-8") as file:
                    self.failed_permanent = json.load(file)
            except (OSError, json.JSONDecodeError):
                self.failed_permanent = {}

    @staticmethod
    def _append_durable(path: Path, text: str) -> None:
        """Ghi thêm (append) và ép sync xuống ổ cứng (fsync) ngay lập tức tránh mất mát."""
        with path.open("a", encoding="utf-8") as file:
            file.write(text)
            file.flush()
            os.fsync(file.fileno())

    @staticmethod
    def _write_json_atomic(path: Path, data: Any) -> None:
        """
        Ghi dữ liệu JSON theo cơ chế Atomic Write:
        Ghi ra file tạm (.tmp) rồi đổi tên (rename/replace) để tránh hỏng file nếu bị tắt đột ngột.
        """
        temp_file = path.with_suffix(path.suffix + ".tmp")
        with temp_file.open("w", encoding="utf-8") as file:
            json.dump(data, file, ensure_ascii=False, indent=2)
            file.flush()
            os.fsync(file.fileno())
        temp_file.replace(path)

    def update_stats(self, last_run_duration: float) -> dict[str, Any]:
        previous_stats: dict[str, Any] = {}
        if self.stats_file.exists():
            try:
                with self.stats_file.open("r", encoding="utf-8") as file:
                    previous_stats = json.load(file)
            except (OSError, json.JSONDecodeError, TypeError):
                previous_stats = {}

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
        """
        Lưu một sản phẩm thành công vào hệ thống:
        - Bỏ qua nếu ID đã được lưu trước đó.
        - Ghi vào file .jsonl (durable) và .progress.txt.
        - Xóa khỏi hàng đợi retry.
        - Cập nhật lại file .json tổng hợp.

        :return: True nếu là sản phẩm mới được lưu, False nếu trùng lặp.
        """
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
        """
        Ghi nhận thông tin thất bại của một request (Fix #4: bọc bằng self._lock):
        - Tăng số lần thử (attempts).
        - Ưu tiên dùng server_retry_after (header Retry-After) nếu có, thay vì BACKOFF_SECONDS (Fix #2).
        - Nếu gặp challenge: đảm bảo luôn đặt global cooldown >= CHALLENGE_MIN_COOLDOWN (Fix #1).
        - Nếu hết lượt retry: ghi ID vào failed_permanent (Fix #5).
        - Nếu stop_all: luôn thiết lập cooldown toàn cục _global (Fix #1).

        :return: Số giây cần chờ trước khi thử lại (0 nếu hết số lần thử hoặc không thể retry).
        """
        async with self._lock:
            key = str(product_id)
            previous_attempts = int(self.retry_state.get(key, {}).get("attempts", 0))
            attempts = previous_attempts + 1

            # Fix #2: ưu tiên server_retry_after nếu server cung cấp
            if server_retry_after is not None and retryable:
                wait_seconds = server_retry_after
                next_retry_at: Optional[float] = time.time() + wait_seconds
            elif retryable and attempts <= len(BACKOFF_SECONDS):
                wait_seconds = BACKOFF_SECONDS[attempts - 1]
                next_retry_at = time.time() + wait_seconds
            else:
                wait_seconds = 0
                next_retry_at = None

            # Fix #1: khi challenge xảy ra và đã hết backoff định sẵn,
            # đặt cooldown tối thiểu cố định thay vì để None → tránh lặp vô hạn
            if stop_all:
                wait_seconds = max(wait_seconds, CHALLENGE_MIN_COOLDOWN)
                next_retry_at = time.time() + wait_seconds

            self.retry_state[key] = {
                "attempts": attempts,
                "reason": reason,
                "next_retry_at": next_retry_at,
            }

            # Fix #1: stop_all luôn ghi _global (không phụ thuộc next_retry_at is not None nữa)
            if stop_all:
                self.retry_state["_global"] = {
                    "reason": reason,
                    "next_retry_at": next_retry_at,
                }

            # Fix #5: nếu hết lượt retry hoặc terminal → ghi vào failed_permanent
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

    def is_due(self, product_id: int) -> bool:
        """
        Kiểm tra xem product_id này đã đến thời điểm được phép retry hay chưa.
        ID đã vào failed_permanent sẽ không bao giờ đến hạn (trả về False).
        """
        # Fix #5: bỏ qua ID đã ghi vào danh sách lỗi vĩnh viễn
        if str(product_id) in self.failed_permanent:
            return False
        state = self.retry_state.get(str(product_id))
        if not state:
            return True
        next_retry_at = state.get("next_retry_at")
        return next_retry_at is not None and time.time() >= float(next_retry_at)

    def global_wait_remaining(self) -> int:
        """
        Tính số giây còn lại của thời gian cooldown toàn cục (nếu bị dính HTML Security Challenge).

        :return: Số giây còn phải chờ (0 nếu không trong thời gian cooldown).
        """
        state = self.retry_state.get("_global")
        if not state:
            return 0
        remaining = float(state.get("next_retry_at", 0)) - time.time()
        if remaining <= 0:
            # Cooldown đã hết, xóa _global để lần sau không bị chặn nữa
            self.retry_state.pop("_global", None)
            self._write_json_atomic(self.retry_file, self.retry_state)
            return 0
        return int(remaining + 0.999)

    @property
    def permanently_failed_count(self) -> int:
        """Số lượng ID lỗi vĩnh viễn (Fix #7: dùng trong log tổng kết)."""
        return len(self.failed_permanent)


def _parse_retry_after(header_value: Optional[str]) -> Optional[int]:
    """
    Parse giá trị header Retry-After từ server:
    - Nếu là số nguyên: coi là số giây cần chờ.
    - Nếu là HTTP-date: tính delta so với thời điểm hiện tại.

    :return: Số giây cần chờ, hoặc None nếu không parse được.
    """
    if not header_value:
        return None
    try:
        return max(1, int(header_value.strip()))
    except ValueError:
        # HTTP-date format (vd: "Fri, 05 Sep 2026 10:00:00 GMT")
        try:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(header_value.strip())
            delta = int(dt.timestamp() - time.time())
            return max(1, delta)
        except Exception:
            return None


async def fetch_product(
    session: aiohttp.ClientSession,
    pacer: NaturalPacer,
    product_id: int,
) -> FetchResult:
    """
    Gửi HTTP GET request đến Tiki API để lấy chi tiết sản phẩm.

    Quy trình xử lý:
    1. Chờ qua pacer (delay ngẫu nhiên).
    2. Gửi request lấy body và headers.
    3. Phân loại phản hồi:
       - 404: Sản phẩm không tồn tại -> dừng (terminal).
       - 403, 429: Bị chặn/quá tải -> retry, đọc Retry-After header nếu có (Fix #2).
       - 5xx: Lỗi server -> retry.
       - HTML hoặc bắt đầu '<': BytePlus WAF challenge -> challenge.
       - JSON thiếu field bắt buộc: soft block -> retry/challenge (Fix #3).
       - JSON hợp lệ: Chuẩn hóa dữ liệu -> success.

    :return: Đối tượng FetchResult chứa trạng thái và dữ liệu tương ứng.
    """
    await pacer.wait()
    url = f"{TIKI_API_BASE_URL}/{product_id}"
    try:
        async with session.get(url) as response:
            body = await response.text()
            content_type = response.headers.get("Content-Type", "").lower()

            # Sản phẩm không tồn tại / đã bị xóa
            if response.status == 404:
                return FetchResult("terminal", reason="http_404")

            # Fix #2: đọc Retry-After header khi bị 429/403
            if response.status in {403, 429}:
                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                return FetchResult(
                    "retry",
                    reason=f"http_{response.status}",
                    retry_after=retry_after,
                )

            # Lỗi máy chủ Tiki
            if response.status >= 500:
                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                return FetchResult(
                    "retry",
                    reason=f"http_{response.status}",
                    retry_after=retry_after,
                )
            if response.status != 200:
                return FetchResult("terminal", reason=f"http_{response.status}")

            # Phát hiện trang HTML Security Challenge (BytePlus/WAF)
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

            # Fix #3: phát hiện "soft block" — status 200 nhưng thiếu field bắt buộc
            # WAF đôi khi trả JSON rỗng/giả để bypass detection
            missing = REQUIRED_PRODUCT_FIELDS - {k for k, v in data.items() if v is not None}
            if missing:
                # Nếu bị trả về JSON không đủ field -> nghi ngờ soft block, retry thận trọng
                return FetchResult(
                    "retry",
                    reason=f"soft_block_missing_fields:{','.join(sorted(missing))}",
                )

            # Chuẩn hóa cấu trúc sản phẩm
            product = extract_product_fields(data)
            product["images_url"] = product.pop("images")
            return FetchResult("success", product=product)
    except asyncio.TimeoutError:
        return FetchResult("retry", reason="timeout")
    except aiohttp.ClientError as exc:
        return FetchResult("retry", reason=type(exc).__name__)


def load_product_ids(input_file: Path, limit: int) -> list[int]:
    """
    Đọc danh sách Product ID từ file văn bản đầu vào:
    - Bỏ qua các dòng trống hoặc không phải là số.
    - Loại bỏ các ID bị trùng lặp (giữ nguyên thứ tự xuất hiện).
    - Cắt theo giới hạn (limit) nếu có truyền vào.

    :param input_file: Đường dẫn file input (vd: data/input/product_ids.txt).
    :param limit: Số lượng tối đa cần lấy (0 nghĩa là lấy toàn bộ).
    :return: Danh sách các ID kiểu số nguyên (int).
    """
    with input_file.open("r", encoding="utf-8") as file:
        ids = [int(line.strip()) for line in file if line.strip().isdigit()]
    ids = list(dict.fromkeys(ids))
    return ids[:limit] if limit else ids


async def run(args: argparse.Namespace) -> None:
    product_ids = load_product_ids(args.input, args.limit)
    await run_product_ids(product_ids, args)


async def run_product_ids(product_ids: list[int], args: argparse.Namespace) -> None:
    """
    Hàm điều phối thực thi chính (Main Orchestrator):
    1. Đọc danh sách ID và kiểm tra dữ liệu đã lưu trong ResultStore.
    2. Kiểm tra nếu hệ thống đang trong thời gian cooldown WAF -> dừng sớm.
    3. Lọc ra các ID còn tồn đọng và đã đến hạn retry (pending_ids), bỏ qua failed_permanent.
    4. Khởi tạo aiohttp ClientSession cùng bộ điều tiết NaturalPacer.
    5. Khởi chạy các worker bất đồng bộ (asyncio tasks) để cào dữ liệu song song an toàn.
    6. Fix #6: khi gặp challenge, cancel các asyncio task còn lại ngay lập tức thay vì đợi chúng hoàn tất.
    7. Fix #7: in log tổng kết tách biệt giữa lỗi tạm thời (retry) và lỗi vĩnh viễn (permanently failed).
    """
    run_start_time = time.time()
    store = ResultStore(args.output)
    counters = {"success": 0, "failed_temp": 0, "failed_perm": 0}

    def print_summary() -> None:
        last_run_duration = time.time() - run_start_time
        stats = store.update_stats(last_run_duration)
        total_perm_failed = store.permanently_failed_count
        print(
            f"\nKết thúc lượt chạy:"
            f"\n  ⏱️ Thời gian lượt này  : {stats['last_run_duration']}"
            f"\n  ⌛ Tổng thời gian tích lũy: "
            f"{stats['total_time_human']} ({stats['total_time_formatted']})"
            f"\n  ⚡ Tốc độ trung bình    : {stats['average_speed']}"
            f"\n  ✅ Mới lưu thành công   : {counters['success']}"
            f"\n  ⏳ Lỗi tạm thời (retry): {counters['failed_temp']}"
            f"\n  ❌ Lỗi vĩnh viễn       : "
            f"{counters['failed_perm']} (tổng tích lũy: {total_perm_failed})"
            f"\n  📦 Tổng đã lưu          : {len(store.products)}"
            f"\n  📄 Output               : {args.output}"
            f"\n  📊 Stats                : {store.stats_file}"
            f"\n  🚫 Permanent fails      : {store.failed_permanent_file}"
        )

    global_wait = store.global_wait_remaining()
    if global_wait:
        print(
            f"Retry queue đang cooldown do HTML challenge. "
            f"Chạy lại sau ít nhất {global_wait}s."
        )
        print_summary()
        return

    pending_ids = [
        product_id
        for product_id in product_ids
        if product_id not in store.successful_ids and store.is_due(product_id)
    ]
    # Fix #7: tách rõ ràng các loại ID tồn đọng
    deferred_retry = sum(
        1 for pid in product_ids
        if pid not in store.successful_ids
        and str(pid) not in store.failed_permanent
        and not store.is_due(pid)
    )
    print(
        f"IDs={len(product_ids)}, đã thành công={len(store.successful_ids)}, "
        f"cần xử lý={len(pending_ids)}, đang backoff={deferred_retry}, "
        f"lỗi vĩnh viễn={store.permanently_failed_count}"
    )
    if not pending_ids:
        print(f"Không có ID đến hạn xử lý. Output: {args.output}")
        print_summary()
        return

    timeout = aiohttp.ClientTimeout(total=args.timeout)
    connector = aiohttp.TCPConnector(
        limit=args.concurrency,
        limit_per_host=args.concurrency,
        ttl_dns_cache=300,
    )
    pacer = NaturalPacer(args.delay_min, args.delay_max)
    stop_event = asyncio.Event()
    id_queue: asyncio.Queue[int] = asyncio.Queue()
    for product_id in pending_ids:
        id_queue.put_nowait(product_id)

    print(f"🚀 Khởi chạy {args.concurrency} worker(s) bất đồng bộ...")
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=timeout,
        headers=DEFAULT_HEADERS,
        cookie_jar=aiohttp.CookieJar(),
    ) as session:
        async def worker(worker_id: int) -> None:
            """Worker async lấy từng ID từ hàng đợi để xử lý cho đến khi hết queue hoặc có lệnh dừng."""
            tag = f"[Worker {worker_id}]"
            while not stop_event.is_set():
                try:
                    product_id = id_queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                result = await fetch_product(session, pacer, product_id)
                if result.status == "success":
                    if await store.save_product(result.product):
                        counters["success"] += 1
                    print(f"{tag} [OK] {product_id} -> đã lưu ngay ({len(store.products)} products)")
                    continue

                wait_seconds = await store.record_failure(
                    product_id,
                    result.reason,
                    retryable=result.status != "terminal",
                    stop_all=result.status == "challenge",
                    server_retry_after=result.retry_after,  # Fix #2
                )

                # Fix #7: phân biệt lỗi tạm thời và vĩnh viễn trong counter
                if result.status == "terminal" or (result.status == "retry" and wait_seconds == 0):
                    counters["failed_perm"] += 1
                else:
                    counters["failed_temp"] += 1

                if result.status == "challenge":
                    # Fix #6: set stop_event và return ngay — các worker khác sẽ thoát ở đầu vòng lặp tiếp theo.
                    # Task được cancel bởi asyncio.gather khi worker đầu tiên gặp challenge raise CancelledError.
                    stop_event.set()
                    print(
                        f"{tag} [STOP] {product_id}: BytePlus HTML challenge. "
                        f"ID được hẹn retry sau {wait_seconds}s; dừng cả lượt chạy."
                    )
                    return
                if wait_seconds:
                    src = f" (Retry-After: {result.retry_after}s)" if result.retry_after else ""
                    print(
                        f"{tag} [RETRY] {product_id}: {result.reason}{src}; "
                        f"hẹn lại sau {wait_seconds}s"
                    )
                else:
                    print(f"{tag} [FAILED] {product_id}: {result.reason}; đã hết retry → ghi permanent")

        # Fix #6: tạo tasks kèm worker_id để theo dõi chi tiết
        tasks = [
            asyncio.create_task(worker(worker_id=i + 1))
            for i in range(args.concurrency)
        ]
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            # Đảm bảo tất cả tasks đã dừng khi bị cancel từ bên ngoài
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # Fix #7: log tổng kết tách biệt lỗi tạm thời và vĩnh viễn
    print_summary()


def parse_args() -> argparse.Namespace:
    """
    Khai báo và kiểm tra tính hợp lệ của các tham số dòng lệnh (CLI Arguments):
    - --input: File chứa danh sách ID đầu vào.
    - --output: File đích lưu kết quả JSON.
    - --limit: Giới hạn số lượng ID xử lý (0 là toàn bộ).
    - --concurrency: Số worker chạy đồng thời (mặc định 5, tối đa 10).
    - --delay-min / --delay-max: Khoảng thời gian giãn cách ngẫu nhiên giữa các request.
    - --timeout: Thời gian timeout cho mỗi HTTP request.
    """
    parser = argparse.ArgumentParser(description="Tiki crawler có checkpoint và retry queue")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--limit", type=int, default=0, help="0 = đọc toàn bộ ID")
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--delay-min", type=float, default=2.0)
    parser.add_argument("--delay-max", type=float, default=8.0)
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()
    if not args.input.exists():
        parser.error(f"Không tìm thấy input: {args.input}")
    if args.limit < 0:
        parser.error("--limit phải >= 0")
    if not 1 <= args.concurrency <= 10:
        parser.error("--concurrency phải từ 1 đến 10")
    if args.delay_min < 0 or args.delay_max < args.delay_min:
        parser.error("Cần 0 <= --delay-min <= --delay-max")
    if args.timeout <= 0:
        parser.error("--timeout phải > 0")
    return args


if __name__ == "__main__":
    asyncio.run(run(parse_args()))
