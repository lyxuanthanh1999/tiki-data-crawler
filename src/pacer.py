"""Request pacing helpers for keeping crawler traffic less bursty."""

import asyncio
import random


class NaturalPacer:
    """
    Serialize request starts and add random delay between them.

    The first request starts immediately. Later requests wait in one shared
    lock, so concurrent workers do not all fire at the same instant.
    """

    def __init__(self, delay_min: float, delay_max: float):
        self.delay_min = delay_min
        self.delay_max = delay_max
        self._lock = asyncio.Lock()
        self._first_request = True

    async def wait(self) -> None:
        async with self._lock:
            if self._first_request:
                self._first_request = False
                return
            await asyncio.sleep(random.uniform(self.delay_min, self.delay_max))
