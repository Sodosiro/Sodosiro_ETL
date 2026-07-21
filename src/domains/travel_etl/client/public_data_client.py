"""공공데이터포털 TourAPI(KorService2) 클라이언트.

적용 패턴
- Template Method: `ApiClientBase.iter_items` 가 페이지 순회 골격을 제공하고
  `get_page` 를 하위 클래스가 구현한다.
- Proxy: `GuardedPublicDataClient` 가 실제 클라이언트 앞에서
  호출 간 최소 간격을 강제하고 호출 수를 기록한다 (호출 방어 로직).
- Builder: `ParamsBuilder` 가 공통 파라미터를 조립한다.
"""
from __future__ import annotations

import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import Any, Iterator

import requests

from src.domains.travel_etl.config.settings import TravelEtlSettings
from src.domains.travel_etl.service.rate_limiter import MinIntervalLimiter

logger = logging.getLogger(__name__)

_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
# 서버가 준 Retry-After 를 그대로 따르면 태스크가 무기한 잠들 수 있다.
# 상한을 넘는 대기는 포기하고 재시도 소진 → pending 큐가 다음 실행에서 이어간다.
_MAX_BACKOFF_SEC = 120.0
_OK_RESULT_CODES = {"0000", "00"}
_NO_DATA_RESULT_CODES = {"03", "0003"}
_QUOTA_RESULT_CODES = {"22", "0022"}  # LIMITED_NUMBER_OF_SERVICE_REQUESTS_EXCEEDS


class Endpoint(str, Enum):
    """사용하는 KorService2 엔드포인트 (공공데이터사용방법.md 기준)."""

    AREA_CODE = "areaCode2"
    CATEGORY_CODE = "categoryCode2"
    AREA_BASED_LIST = "areaBasedList2"
    SEARCH_KEYWORD = "searchKeyword2"
    DETAIL_COMMON = "detailCommon2"
    DETAIL_INTRO = "detailIntro2"
    DETAIL_INFO = "detailInfo2"
    DETAIL_IMAGE = "detailImage2"
    LCLS_SYSTM_CODE = "lclsSystmCode2"
    LDONG_CODE = "ldongCode2"


class PublicDataApiError(RuntimeError):
    """resultCode 가 정상이 아닌 응답."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"공공데이터 API 오류 [{code}] {message}")
        self.code = code


class PublicDataQuotaError(PublicDataApiError):
    """원천 서버가 호출 한도 초과(resultCode 22)를 반환한 경우."""


class PublicDataRateLimitError(PublicDataApiError):
    """HTTP 429 재시도를 소진한 경우. pending 작업은 다음 실행에서 이어간다."""


@dataclass(frozen=True)
class Page:
    """한 페이지 응답."""

    items: list[dict]
    total_count: int
    page_no: int


class ParamsBuilder:
    """요청 파라미터 Builder — 공통 파라미터를 채우고 체이닝으로 조립한다."""

    def __init__(self, api_key: str) -> None:
        self._params: dict[str, Any] = {
            "serviceKey": api_key,
            "MobileOS": "ETC",
            "MobileApp": "sodosiro",
            "_type": "json",
        }

    def with_paging(self, page_no: int, num_rows: int) -> "ParamsBuilder":
        self._params.update(pageNo=page_no, numOfRows=num_rows)
        return self

    def with_params(self, **params: Any) -> "ParamsBuilder":
        self._params.update({k: v for k, v in params.items() if v is not None})
        return self

    def build(self) -> dict[str, Any]:
        return dict(self._params)


class ApiClientBase(ABC):
    """페이지 순회 골격 (Template Method)."""

    @abstractmethod
    def get_page(self, endpoint: Endpoint, page_no: int, num_rows: int, **params: Any) -> Page:
        """한 페이지를 조회한다."""

    def iter_items(self, endpoint: Endpoint, num_rows: int, **params: Any) -> Iterator[dict]:
        """totalCount 를 기준으로 모든 페이지의 item 을 순회한다."""
        page_no = 1
        while True:
            page = self.get_page(endpoint, page_no, num_rows, **params)
            logger.debug(
                "%s p%d: %d건 / 전체 %d건",
                endpoint.value, page.page_no, len(page.items), page.total_count,
            )
            yield from page.items
            if page_no * num_rows >= page.total_count or not page.items:
                return
            page_no += 1

    def get_single(self, endpoint: Endpoint, **params: Any) -> dict | None:
        """단건 조회 (detailCommon2 등) — 첫 item 을 반환한다."""
        page = self.get_page(endpoint, page_no=1, num_rows=10, **params)
        return page.items[0] if page.items else None


class PublicDataClient(ApiClientBase):
    """실제 HTTP 호출 + 재시도(지수 백오프) + 응답 파싱."""

    def __init__(self, settings: TravelEtlSettings, session: requests.Session | None = None) -> None:
        self._settings = settings
        self._session = session or requests.Session()

    def get_page(self, endpoint: Endpoint, page_no: int, num_rows: int, **params: Any) -> Page:
        query = (
            ParamsBuilder(self._settings.public_data_api_key)
            .with_paging(page_no, num_rows)
            .with_params(**params)
            .build()
        )
        payload = self._request_with_retry(endpoint, query)
        return self._parse_page(payload, page_no)

    def _request_with_retry(self, endpoint: Endpoint, query: dict) -> dict:
        url = f"{self._settings.public_data_base_url}/{endpoint.value}"
        last_error: Exception | None = None
        for attempt in range(self._settings.max_retries + 1):
            try:
                return self._request_once(url, query)
            except (requests.ConnectionError, requests.Timeout, _RetryableHttpError) as e:
                last_error = e
                if attempt < self._settings.max_retries:
                    self._backoff(attempt, endpoint, e)
        if isinstance(last_error, _RetryableHttpError) and last_error.status_code == 429:
            raise PublicDataRateLimitError("429", f"{endpoint.value}: 요청 속도 제한")
        raise PublicDataApiError("RETRY_EXHAUSTED", f"{endpoint.value}: {last_error}")

    def _request_once(self, url: str, query: dict) -> dict:
        response = self._session.get(url, params=query, timeout=self._settings.request_timeout_sec)
        if response.status_code in _RETRYABLE_STATUS:
            raise _RetryableHttpError(
                response.status_code,
                _retry_after_seconds(response.headers.get("Retry-After")),
            )
        response.raise_for_status()
        return self._decode_json(response)

    @staticmethod
    def _decode_json(response: requests.Response) -> dict:
        try:
            return response.json()
        except ValueError as e:
            # 인증키 오류 등은 XML(OpenAPI_ServiceResponse)로 내려온다.
            raise PublicDataApiError("NON_JSON", response.text[:300]) from e

    @staticmethod
    def _backoff(attempt: int, endpoint: Endpoint, error: Exception) -> None:
        if isinstance(error, _RetryableHttpError) and error.retry_after_sec is not None:
            delay = error.retry_after_sec
        elif isinstance(error, _RetryableHttpError) and error.status_code == 429:
            delay = 5 * (2**attempt)
        else:
            delay = 2**attempt
        delay = min(delay, _MAX_BACKOFF_SEC)
        logger.warning("%s 재시도 %d회차 (%s) — %.0fs 대기", endpoint.value, attempt + 1, error, delay)
        time.sleep(delay)

    @staticmethod
    def _parse_page(payload: dict, page_no: int) -> Page:
        header = payload.get("response", {}).get("header", {})
        code = str(header.get("resultCode", "")).strip()
        if code in _QUOTA_RESULT_CODES:
            raise PublicDataQuotaError(code, str(header.get("resultMsg")))
        if code not in _OK_RESULT_CODES:
            if code in _NO_DATA_RESULT_CODES:
                return Page(items=[], total_count=0, page_no=page_no)
            raise PublicDataApiError(code or "UNKNOWN", str(header.get("resultMsg")))
        body = payload.get("response", {}).get("body", {}) or {}
        return Page(
            items=_extract_items(body),
            total_count=int(body.get("totalCount") or 0),
            page_no=page_no,
        )


class _RetryableHttpError(RuntimeError):
    """429/5xx 등 재시도 대상 HTTP 상태."""

    def __init__(self, status_code: int, retry_after_sec: float | None = None) -> None:
        super().__init__(f"HTTP {status_code}")
        self.status_code = status_code
        self.retry_after_sec = retry_after_sec


def _retry_after_seconds(value: str | None) -> float | None:
    """Retry-After의 초 또는 HTTP-date 형식을 대기 초로 변환한다."""
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            retry_at = parsedate_to_datetime(value)
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            return max(0.0, (retry_at - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None


def _extract_items(body: dict) -> list[dict]:
    """items 가 ""(빈 문자열)·단건 dict·list 인 경우를 모두 흡수한다."""
    items = body.get("items")
    if not items or not isinstance(items, dict):
        return []
    item = items.get("item")
    if isinstance(item, list):
        return item
    return [item] if isinstance(item, dict) else []


class GuardedPublicDataClient(ApiClientBase):
    """호출 pacing Proxy — 모든 호출 전에 최소 간격을 강제하고 호출 수를 기록한다."""

    def __init__(
        self,
        inner: ApiClientBase,
        limiter: MinIntervalLimiter,
    ) -> None:
        self._inner = inner
        self._limiter = limiter
        self.calls_made = 0

    def get_page(self, endpoint: Endpoint, page_no: int, num_rows: int, **params: Any) -> Page:
        self._limiter.wait()
        self.calls_made += 1
        return self._inner.get_page(endpoint, page_no, num_rows, **params)
