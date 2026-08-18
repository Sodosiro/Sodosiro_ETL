"""인기 검색어 Redis → DB sync 서비스.

Redis 일별 버킷(서빙 소스)을 최근 N일치 읽어 ``search_keyword_trend`` 스냅샷
테이블로 멱등 동기화한다. Redis 재시작 대비 내구성 확보 + 검색어 추이 분석용이며,
조회 경로(BE)는 계속 Redis 를 본다.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import pendulum

from src.domains.search_trend.config.settings import (
    SearchTrendSettings,
    get_search_trend_settings,
)
from src.domains.search_trend.repository.redis_reader import RedisTrendReader
from src.domains.search_trend.repository.search_trend_repository import (
    SearchTrendConnectionFactory,
    SearchTrendRow,
)

logger = logging.getLogger(__name__)

_KST = pendulum.timezone("Asia/Seoul")


class SearchTrendSyncService:
    """인기 검색어 sync 유스케이스."""

    def __init__(self, settings: SearchTrendSettings | None = None) -> None:
        self._settings = settings or get_search_trend_settings()
        self._db = SearchTrendConnectionFactory(self._settings.db_url)

    def sync_recent_buckets(self, end_date: date | None = None) -> dict:
        """end_date(기본: 오늘 KST) 기준 최근 window_days 일 버킷을 DB 로 sync 한다."""
        settings = self._settings
        anchor = end_date or pendulum.now(_KST).date()
        reader = RedisTrendReader(settings.redis_url, settings.bucket_prefix)
        try:
            rows: list[SearchTrendRow] = []
            days_with_data = 0
            for offset in range(settings.window_days):
                bucket_date = anchor - timedelta(days=offset)
                day_rows = reader.read_day(bucket_date, settings.per_day_limit)
                if day_rows:
                    days_with_data += 1
                rows.extend(
                    SearchTrendRow(bucket_date, kc.keyword, kc.count) for kc in day_rows
                )
        finally:
            reader.close()

        with self._db.open() as repo:
            upserted = repo.upsert(rows, settings.upsert_batch_size)

        result = {
            "anchor_date": anchor.isoformat(),
            "window_days": settings.window_days,
            "days_with_data": days_with_data,
            "keywords_upserted": upserted,
        }
        logger.info("[SearchTrendSync] %s", result)
        return result
