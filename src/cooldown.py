"""Chờ và kiểm tra cooldown khi Tiki trả về WAF/HTML challenge."""

import logging
import time

from fetch_tiki_products import ResultStore

logger = logging.getLogger("Main")


def wait_for_cooldown(store: ResultStore, batch_index: int, auto_wait_interval: float, max_retries: int) -> bool:
    """Chờ cooldown WAF theo lịch tăng dần; trả False khi vượt max retry."""
    short_checks = 0
    waf_retries = 0
    had_cooldown = False
    while True:
        cooldown = store.global_wait_remaining()
        if cooldown <= 0:
            if not had_cooldown:
                return True
            if short_checks == 0:
                short_checks = 1
                logger.info("Batch #%04d: cooldown kết thúc, kiểm tra lại sau 3 phút", batch_index)
                time.sleep(3 * 60)
                continue
            logger.info("Batch #%04d: WAF đã clear, tiếp tục xử lý", batch_index)
            return True

        waf_retries += 1
        had_cooldown = True
        if max_retries and waf_retries > max_retries:
            logger.error("Batch #%04d: vượt quá %s lần chờ WAF", batch_index, max_retries)
            return False

        sleep_seconds = min(cooldown, max(5, int(auto_wait_interval)))
        logger.warning(
            "Batch #%04d: WAF cooldown còn %ss (~%.1f phút), ngủ %ss rồi kiểm tra lại",
            batch_index, cooldown, cooldown / 60, sleep_seconds,
        )
        time.sleep(sleep_seconds)
