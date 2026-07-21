"""Spring 서버 콜백 클라이언트.

적재가 끝난 뒤 변경된 여행지 content_id 목록을 Spring(DATA_EXTRACT 도메인)에 전달한다.
이후의 AI 처리(대표 키워드·LLM·Spring AI 임베딩 → spot_embedding)는 Spring 책임이며,
처리가 끝난 건은 Spring 이 etl_spot_state.embed_pending 을 false 로 내린다.

계약 (docs/travel-ai-handoff.md):
    POST {SPRING_BASE_URL}/internal/etl/travel/refresh
    {"runId": "...", "contentIds": [126508, ...]}
    → 2xx {"accepted": <건수>}

Spring 이 내려가 있어도 embed_pending 이 큐 역할을 하므로 다음 실행에서 다시 통지된다.
"""
from __future__ import annotations

import logging
import time

import requests

from src.domains.travel_etl.config.settings import TravelEtlSettings

logger = logging.getLogger(__name__)


class SpringNotifyError(RuntimeError):
    """Spring 콜백 실패 (재시도 소진)."""


class SpringClient:
    """변경분 통지 — 배치 분할 + 일시 오류(5xx·연결) 지수 백오프 재시도."""

    def __init__(self, settings: TravelEtlSettings, session: requests.Session | None = None) -> None:
        self._settings = settings
        self._session = session or requests.Session()

    def notify_changed_spots(self, run_id: str, content_ids: list[int]) -> int:
        """content_id 를 배치로 나눠 전달하고 수락 건수를 반환한다."""
        accepted = 0
        batch_size = self._settings.spring_notify_batch
        for start in range(0, len(content_ids), batch_size):
            accepted += self._notify_once(run_id, content_ids[start : start + batch_size])
        return accepted

    def _notify_once(self, run_id: str, batch: list[int]) -> int:
        url = f"{self._settings.spring_base_url}/internal/etl/travel/refresh"
        payload = {"runId": run_id, "contentIds": batch}
        last_error: Exception | None = None
        for attempt in range(self._settings.max_retries + 1):
            try:
                return self._post(url, payload)
            except (requests.ConnectionError, requests.Timeout, _RetryableStatus) as e:
                last_error = e
                delay = 2**attempt
                logger.warning("Spring 통지 재시도 %d회차 (%s) — %.0fs 대기", attempt + 1, e, delay)
                time.sleep(delay)
        raise SpringNotifyError(f"Spring 통지 실패: {last_error}")

    def _post(self, url: str, payload: dict) -> int:
        response = self._session.post(
            url,
            json=payload,
            auth=self._auth(),
            timeout=self._settings.spring_timeout_sec,
        )
        if response.status_code >= 500:
            raise _RetryableStatus(f"HTTP {response.status_code}")
        if response.status_code >= 400:
            raise SpringNotifyError(f"HTTP {response.status_code}: {response.text[:200]}")
        body = response.json() if response.content else {}
        return int(body.get("accepted", len(payload["contentIds"])))

    def _auth(self) -> tuple[str, str] | None:
        username = self._settings.spring_api_username
        return (username, self._settings.spring_api_password) if username else None


class _RetryableStatus(RuntimeError):
    """5xx — 재시도 대상."""
