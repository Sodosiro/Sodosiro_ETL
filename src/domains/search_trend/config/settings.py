"""인기 검색어 sync 설정.

환경변수 / .env 파일에서 값을 읽는다 (travel_etl·trend 설정과 동일한 패턴).
BE(Redis Sorted Set 서빙)와 키·윈도우를 반드시 일치시켜야 한다.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _build_db_url() -> str:
    url = _env("TRAVEL_DB_URL")
    if url:
        return url
    user = _env("POSTGRESS_USERNAME", "postgres")
    password = _env("POSTGRESS_PASSWORD", "")
    host = _env("TRAVEL_DB_HOST", "localhost")
    port = _env("TRAVEL_DB_PORT", "5432")
    database = _env("POSTGRESS_DATABASE", "travel")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def _build_redis_url() -> str:
    """REDIS_URL 이 있으면 그대로, 없으면 조각으로 조립한다.

    BE 의 Spring Data Redis 와 동일한 인스턴스를 가리켜야 한다.
    """
    url = _env("REDIS_URL")
    if url:
        return url
    host = _env("REDIS_HOST", "localhost")
    port = _env("REDIS_PORT", "6379")
    db = _env("REDIS_DATABASE", "0")
    password = _env("REDIS_PASSWORD")
    auth = f":{password}@" if password else ""
    return f"redis://{auth}{host}:{port}/{db}"


@dataclass(frozen=True)
class SearchTrendSettings:
    """인기 검색어 sync 전역 설정 (불변)."""

    db_url: str
    redis_url: str
    bucket_prefix: str      # BE 와 동일해야 함 (travel:search:trending:)
    window_days: int        # sync 대상 최근 일수 (BE WINDOW_DAYS 와 일치)
    per_day_limit: int      # 하루 버킷에서 스냅샷할 최대 상위 검색어 수
    upsert_batch_size: int

    @classmethod
    def from_env(cls) -> "SearchTrendSettings":
        return cls(
            db_url=_build_db_url(),
            redis_url=_build_redis_url(),
            bucket_prefix=_env("SEARCH_TREND_BUCKET_PREFIX", "travel:search:trending:"),
            window_days=int(_env("SEARCH_TREND_WINDOW_DAYS", "30")),
            per_day_limit=int(_env("SEARCH_TREND_PER_DAY_LIMIT", "500")),
            upsert_batch_size=int(_env("SEARCH_TREND_UPSERT_BATCH", "500")),
        )


def _load_env_file() -> None:
    env_file = _PROJECT_ROOT / ".env"
    if not env_file.is_file():
        return
    for line in env_file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not os.environ.get(key):
            os.environ[key] = value.strip()


@lru_cache(maxsize=1)
def get_search_trend_settings() -> SearchTrendSettings:
    _load_env_file()
    return SearchTrendSettings.from_env()
