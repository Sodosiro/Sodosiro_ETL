"""호출 간 최소 간격을 강제하는 공통 pacing 도구."""
from __future__ import annotations

import time


class MinIntervalLimiter:
    """호출 간 최소 간격 보장 (단순 pacing)."""

    def __init__(self, min_interval_sec: float) -> None:
        self._min_interval = min_interval_sec
        self._last_call = 0.0

    def wait(self) -> None:
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_interval:
            time.sleep(self._min_interval - elapsed)
        self._last_call = time.monotonic()
