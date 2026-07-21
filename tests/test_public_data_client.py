"""High 리뷰 수정 검증 — 백오프 대기 상한(H-4)과 기존 재시도 동작 회귀."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from src.domains.travel_etl.client.public_data_client import (
    _MAX_BACKOFF_SEC,
    Endpoint,
    PublicDataClient,
    _extract_items,
    _RetryableHttpError,
)


class BackoffTest(unittest.TestCase):
    def _run_backoff(self, error: Exception, attempt: int = 0) -> float:
        with patch("src.domains.travel_etl.client.public_data_client.time.sleep") as sleep:
            PublicDataClient._backoff(attempt, Endpoint.AREA_BASED_LIST, error)
        return sleep.call_args[0][0]

    def test_큰_Retry_After는_상한으로_잘린다(self) -> None:
        error = _RetryableHttpError(429, retry_after_sec=3600.0)
        self.assertEqual(self._run_backoff(error), _MAX_BACKOFF_SEC)

    def test_상한_이하의_Retry_After는_그대로_따른다(self) -> None:
        error = _RetryableHttpError(429, retry_after_sec=7.0)
        self.assertEqual(self._run_backoff(error), 7.0)

    def test_Retry_After_없는_429는_지수_백오프(self) -> None:
        error = _RetryableHttpError(429)
        self.assertEqual(self._run_backoff(error, attempt=1), 10.0)  # 5 * 2^1

    def test_5xx는_기본_지수_백오프(self) -> None:
        error = _RetryableHttpError(503)
        self.assertEqual(self._run_backoff(error, attempt=2), 4.0)  # 2^2

    def test_지수_백오프도_상한을_넘지_않는다(self) -> None:
        error = _RetryableHttpError(429)
        self.assertEqual(self._run_backoff(error, attempt=10), _MAX_BACKOFF_SEC)


class ExtractItemsTest(unittest.TestCase):
    """응답 방어 파싱 회귀 — items 가 ""/dict/list 인 케이스."""

    def test_빈_문자열(self) -> None:
        self.assertEqual(_extract_items({"items": ""}), [])

    def test_단건_dict(self) -> None:
        self.assertEqual(_extract_items({"items": {"item": {"a": 1}}}), [{"a": 1}])

    def test_목록(self) -> None:
        self.assertEqual(_extract_items({"items": {"item": [{"a": 1}, {"a": 2}]}}), [{"a": 1}, {"a": 2}])


if __name__ == "__main__":
    unittest.main()
