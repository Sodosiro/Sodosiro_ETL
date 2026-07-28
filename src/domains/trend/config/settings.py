"""인기 관광지 트렌드 수집 설정.

환경변수 / .env 파일에서 값을 읽는다 (travel_etl 설정과 동일한 패턴).
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


@dataclass(frozen=True)
class TrendSettings:
    """트렌드 수집 전역 설정 (불변)."""

    kakao_rest_api_key: str
    db_url: str
    snapshot_dir: str

    # 수집·집계 파라미터
    min_frequency: int
    top_n: int
    search_radius_meters: int
    channel_diversity_bonus: float
    city_sleep_sec: float      # 도시 간 슬립 (500ms)
    keyword_sleep_sec: float   # 모든 카카오 API 호출 간 최소 간격 (기본 200ms = 초당 최대 5회)

    # HTTP 클라이언트
    request_timeout_sec: float
    max_retries: int

    # 감쇠 파라미터
    decay_threshold: float
    decay_attraction: float   # AT4·AD5
    decay_restaurant: float   # FD6
    decay_cafe: float         # CE7

    # Spring 통지
    spring_base_url: str
    spring_internal_etl_token: str
    spring_timeout_sec: float

    @classmethod
    def from_env(cls) -> "TrendSettings":
        return cls(
            kakao_rest_api_key=_env("KAKAO_REST_API_KEY"),
            db_url=_build_db_url(),
            snapshot_dir=_env(
                "TREND_SNAPSHOT_DIR",
                str(_PROJECT_ROOT / "data" / "trend" / "raw"),
            ),
            min_frequency=int(_env("TREND_MIN_FREQUENCY", "3")),
            top_n=int(_env("TREND_TOP_N", "20")),
            search_radius_meters=int(_env("TREND_SEARCH_RADIUS", "30000")),
            channel_diversity_bonus=float(_env("TREND_CHANNEL_DIVERSITY_BONUS", "1.3")),
            city_sleep_sec=float(_env("TREND_CITY_SLEEP_SEC", "0.5")),
            keyword_sleep_sec=float(_env("TREND_KEYWORD_SLEEP_SEC", "0.2")),
            request_timeout_sec=float(_env("TREND_REQUEST_TIMEOUT_SEC", "10")),
            max_retries=int(_env("TREND_MAX_RETRIES", "3")),
            decay_threshold=float(_env("TREND_DECAY_THRESHOLD", "0.01")),
            decay_attraction=float(_env("TREND_DECAY_ATTRACTION", "0.85")),
            decay_restaurant=float(_env("TREND_DECAY_RESTAURANT", "0.70")),
            decay_cafe=float(_env("TREND_DECAY_CAFE", "0.70")),
            spring_base_url=_env("SPRING_BASE_URL"),
            spring_internal_etl_token=_env("SPRING_INTERNAL_ETL_TOKEN"),
            spring_timeout_sec=float(_env("SPRING_TIMEOUT_SEC", "10")),
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
def get_trend_settings() -> TrendSettings:
    _load_env_file()
    return TrendSettings.from_env()
