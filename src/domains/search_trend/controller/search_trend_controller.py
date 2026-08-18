"""Airflow DAG 태스크의 얇은 진입점 (Facade).

Controller 는 입력을 service 로 위임한다. Airflow DAG 에는 업무 구현이 노출되지 않는다.
"""
from __future__ import annotations

from datetime import date

from src.domains.search_trend.config.settings import SearchTrendSettings
from src.domains.search_trend.service.search_trend_sync_service import (
    SearchTrendSyncService,
)


class SearchTrendController:
    """인기 검색어 sync 태스크의 얇은 진입점."""

    def __init__(self, settings: SearchTrendSettings | None = None) -> None:
        self._service = SearchTrendSyncService(settings)

    def sync_recent_buckets(self, end_date: date | None = None) -> dict:
        return self._service.sync_recent_buckets(end_date)
