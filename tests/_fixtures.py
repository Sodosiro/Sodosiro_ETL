"""테스트 공용 더미 객체."""
from __future__ import annotations

from contextlib import contextmanager

from src.domains.travel_etl.config.settings import TravelEtlSettings


def make_settings(**overrides) -> TravelEtlSettings:
    values = dict(
        public_data_base_url="http://api.test",
        public_data_api_key="test-key",
        db_url="postgresql://test:test@localhost:5432/test",
        content_type_id=None,
        ldong_regn_code="51",
        page_size=500,
        min_call_interval_sec=0.0,
        detail_batch_limit=300,
        image_recovery_batch_limit=500,
        request_timeout_sec=1.0,
        max_retries=3,
        spring_base_url="http://spring.test",
        spring_api_username="",
        spring_api_password="",
        spring_notify_limit=10,
        spring_notify_batch=500,
        spring_timeout_sec=1.0,
    )
    values.update(overrides)
    return TravelEtlSettings(**values)


class FakeRepo:
    """호출 순서를 기록하는 저장소 더미."""

    def __init__(
        self,
        sigungu_codes: list[str] | None = None,
        area_codes: list[str] | None = None,
    ) -> None:
        self.calls: list[tuple] = []
        self._sigungu_codes = sigungu_codes or []
        self._area_codes = area_codes or []

    def clear_pending(self, kind: str, content_ids: list[int]) -> None:
        self.calls.append(("clear_pending", kind, list(content_ids)))

    def commit(self) -> None:
        self.calls.append(("commit",))

    def rollback(self) -> None:
        self.calls.append(("rollback",))

    def list_sigungu_codes(self, area_code: str) -> list[str]:
        self.calls.append(("list_sigungu_codes", area_code))
        return self._sigungu_codes

    def list_area_codes(self) -> list[str]:
        return self._area_codes

    def upsert_area_codes(self, rows: list) -> int:
        self.calls.append(("upsert_area_codes", rows))
        return len(rows)

    def upsert_sigungu_codes(self, rows: list) -> int:
        self.calls.append(("upsert_sigungu_codes", rows))
        return len(rows)


class FakeClient:
    """엔드포인트·파라미터별 canned 응답을 돌려주는 클라이언트 더미.

    responses 의 키는 (endpoint, 파라미터 튜플) — 파라미터 없는 호출은 빈 튜플.
    """

    def __init__(self, responses: dict) -> None:
        self._responses = responses
        self.requests: list[tuple] = []

    def iter_items(self, endpoint, num_rows: int, **params):
        key = (endpoint, tuple(sorted((k, v) for k, v in params.items() if v is not None)))
        self.requests.append(key)
        yield from self._responses.get(key, [])


class FakeConnectionFactory:
    def __init__(self, repo: FakeRepo) -> None:
        self._repo = repo

    @contextmanager
    def open(self):
        yield self._repo
