"""원천 API 호출 간 최소 간격을 강제하는 pacing 도구.

구현은 core.rate_limiter 로 이동했습니다 — 하위 호환 재내보내기.
"""
from src.core.rate_limiter import MinIntervalLimiter

__all__ = ["MinIntervalLimiter"]
