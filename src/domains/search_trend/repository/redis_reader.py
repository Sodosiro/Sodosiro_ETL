"""인기 검색어 Redis 읽기 계층.

BE 가 ZINCRBY 로 누적하는 일별 Sorted Set 버킷
``{bucket_prefix}{yyyyMMdd}`` 을 읽어 (검색어, 횟수) 목록으로 돌려준다.
BE 는 이 버킷을 서빙 소스로 계속 사용하며, 여기서는 읽기만 한다.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date

import redis

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class KeywordCount:
    keyword: str
    count: int


class RedisTrendReader:
    """일별 검색어 버킷 리더."""

    _DATE_FMT = "%Y%m%d"

    def __init__(self, redis_url: str, bucket_prefix: str) -> None:
        # decode_responses=True → member 를 str 로 바로 받는다.
        self._client = redis.Redis.from_url(redis_url, decode_responses=True)
        self._bucket_prefix = bucket_prefix

    def bucket_key(self, bucket_date: date) -> str:
        return f"{self._bucket_prefix}{bucket_date.strftime(self._DATE_FMT)}"

    def read_day(self, bucket_date: date, limit: int) -> list[KeywordCount]:
        """해당 날짜 버킷의 상위 ``limit`` 개 (검색어, 횟수)를 score 내림차순으로 반환."""
        key = self.bucket_key(bucket_date)
        rows = self._client.zrevrange(key, 0, limit - 1, withscores=True)
        return [KeywordCount(keyword=member, count=int(round(score))) for member, score in rows]

    def ping(self) -> bool:
        return bool(self._client.ping())

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001 - 정리 실패는 무시
            pass
