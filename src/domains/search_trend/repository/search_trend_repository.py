"""인기 검색어 스냅샷 DB 접근 계층 (Repository).

- ``search_keyword_trend`` 에 (bucket_date, keyword) 자연키 UPSERT 로 멱등 적재한다.
- 대량 적재는 execute_values 배치로 왕복을 최소화한다.
- 테이블은 database/migrations/20260818_search_keyword_trend.sql 과 일치한다.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date
from typing import Iterator

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SearchTrendRow:
    bucket_date: date
    keyword: str
    search_count: int


class SearchTrendConnectionFactory:
    """DB 커넥션 생성 책임 분리 (Factory)."""

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url

    @contextmanager
    def open(self) -> Iterator["SearchTrendRepository"]:
        conn = psycopg2.connect(self._db_url)
        try:
            yield SearchTrendRepository(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


class SearchTrendRepository:
    """search_keyword_trend 저장소."""

    def __init__(self, conn) -> None:
        self._conn = conn

    def upsert(self, rows: list[SearchTrendRow], batch_size: int) -> int:
        """(bucket_date, keyword) 기준 UPSERT. 적재/갱신된 행 수를 반환한다."""
        if not rows:
            return 0
        affected = 0
        with self._conn.cursor() as cur:
            for start in range(0, len(rows), batch_size):
                chunk = rows[start:start + batch_size]
                psycopg2.extras.execute_values(
                    cur,
                    """INSERT INTO search_keyword_trend
                           (bucket_date, keyword, search_count, synced_at)
                       VALUES %s
                       ON CONFLICT (bucket_date, keyword) DO UPDATE
                           SET search_count = EXCLUDED.search_count,
                               synced_at    = now()""",
                    [(r.bucket_date, r.keyword, r.search_count) for r in chunk],
                    template="(%s, %s, %s, now())",
                )
                affected += len(chunk)
        return affected
