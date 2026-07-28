"""카카오 검색·로컬 API 클라이언트.

패턴:
- Proxy: GuardedKakaoClient 가 실제 클라이언트 앞에서 호출 간 최소 간격을 강제한다.
- 지수 백오프: 429/5xx 는 max_retries 횟수만큼 재시도한다.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Optional

import requests

from src.core.rate_limiter import MinIntervalLimiter

logger = logging.getLogger(__name__)

VALID_CATEGORIES: frozenset[str] = frozenset({"AT4", "FD6", "CE7", "AD5"})
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_MAX_BACKOFF_SEC = 120.0

_SEARCH_BASE_URL = "https://dapi.kakao.com/v2/search"
_LOCAL_URL = "https://dapi.kakao.com/v2/local/search/keyword.json"


@dataclass(frozen=True)
class KakaoPlace:
    """카카오맵 로컬 검색으로 확인된 장소."""

    kakao_place_id: str
    place_name: str
    category_group_code: str
    category_group_name: str
    address_name: str
    longitude: float
    latitude: float


class KakaoApiError(RuntimeError):
    """재시도 소진 후 최종 실패."""


class KakaoClient:
    """카카오 Daum 검색(블로그·카페) + 카카오맵 로컬 키워드 검색 클라이언트."""

    def __init__(
        self,
        api_key: str,
        timeout: float = 10.0,
        max_retries: int = 3,
        session: requests.Session | None = None,
    ) -> None:
        self._timeout = timeout
        self._max_retries = max_retries
        self._session = session or requests.Session()
        self._session.headers.update({"Authorization": f"KakaoAK {api_key}"})

    def search_blog(self, query: str, size: int = 50) -> list[dict]:
        return self._search("blog", query, size)

    def search_cafe(self, query: str, size: int = 50) -> list[dict]:
        return self._search("cafe", query, size)

    def search_local(
        self,
        keyword: str,
        x: float,
        y: float,
        radius: int = 30000,
    ) -> Optional[KakaoPlace]:
        """키워드 검색 — 첫 번째 유효 카테고리(AT4·FD6·CE7·AD5) 장소 반환."""
        data = self._get_with_retry(_LOCAL_URL, {"query": keyword, "x": x, "y": y, "radius": radius})
        for doc in data.get("documents", []):
            code = doc.get("category_group_code", "")
            if code in VALID_CATEGORIES:
                return KakaoPlace(
                    kakao_place_id=doc["id"],
                    place_name=doc["place_name"],
                    category_group_code=code,
                    category_group_name=doc.get("category_group_name", ""),
                    address_name=doc.get("road_address_name") or doc.get("address_name", ""),
                    longitude=float(doc["x"]),
                    latitude=float(doc["y"]),
                )
        return None

    def _search(self, channel: str, query: str, size: int) -> list[dict]:
        data = self._get_with_retry(f"{_SEARCH_BASE_URL}/{channel}", {"query": query, "size": size})
        return data.get("documents", [])

    def _get_with_retry(self, url: str, params: dict) -> dict:
        last_error: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
                if resp.status_code in _RETRYABLE_STATUS:
                    raise _RetryableHttpError(resp.status_code)
                resp.raise_for_status()
                return resp.json()
            except (requests.ConnectionError, requests.Timeout, _RetryableHttpError) as e:
                last_error = e
                if attempt < self._max_retries:
                    delay = _calc_backoff(attempt, e)
                    logger.warning("Kakao API 재시도 %d회차 (%s) — %.0fs 대기", attempt + 1, e, delay)
                    time.sleep(delay)
        raise KakaoApiError(f"Kakao API 요청 실패 ({url}): {last_error}")


class GuardedKakaoClient:
    """호출 pacing Proxy — 모든 호출 전에 최소 간격을 강제하고 호출 수를 기록한다."""

    def __init__(self, inner: KakaoClient, limiter: MinIntervalLimiter) -> None:
        self._inner = inner
        self._limiter = limiter
        self.calls_made = 0

    def search_blog(self, query: str, size: int = 50) -> list[dict]:
        return self._guarded(self._inner.search_blog, query, size)

    def search_cafe(self, query: str, size: int = 50) -> list[dict]:
        return self._guarded(self._inner.search_cafe, query, size)

    def search_local(
        self, keyword: str, x: float, y: float, radius: int = 30000
    ) -> Optional[KakaoPlace]:
        self._limiter.wait()
        self.calls_made += 1
        return self._inner.search_local(keyword, x, y, radius)

    def _guarded(self, method, *args) -> list[dict]:
        self._limiter.wait()
        self.calls_made += 1
        return method(*args)


class _RetryableHttpError(RuntimeError):
    def __init__(self, status_code: int) -> None:
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code


def _calc_backoff(attempt: int, error: Exception) -> float:
    if isinstance(error, _RetryableHttpError) and error.status_code == 429:
        delay = 5.0 * (2**attempt)
    else:
        delay = float(2**attempt)
    return min(delay, _MAX_BACKOFF_SEC)
