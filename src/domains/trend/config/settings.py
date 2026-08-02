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
    search_page_size: int
    search_max_pages: int
    search_radius_meters: int
    channel_diversity_bonus: float
    city_sleep_sec: float
    keyword_sleep_sec: float

    # HTTP 클라이언트
    request_timeout_sec: float
    max_retries: int

    # 인기도 점수 가중치
    mention_weight: float       # 카카오 언급 점수 가중치
    like_weight: float          # 좋아요 수 가중치
    review_weight: float        # 리뷰 수 가중치
    rating_weight: float        # 평점 × 리뷰 수 가중치

    # 감쇠 및 랭킹
    mention_decay_factor: float  # 실행마다 mention_score 에 곱하는 감쇠 계수
    popularity_top_rank_n: int   # 카테고리별 rank_tag 부여 상위 N위

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
            search_page_size=int(_env("TREND_SEARCH_PAGE_SIZE", "50")),
            search_max_pages=int(_env("TREND_SEARCH_MAX_PAGES", "2")),
            search_radius_meters=int(_env("TREND_SEARCH_RADIUS", "30000")),
            channel_diversity_bonus=float(_env("TREND_CHANNEL_DIVERSITY_BONUS", "1.3")),
            city_sleep_sec=float(_env("TREND_CITY_SLEEP_SEC", "0.5")),
            keyword_sleep_sec=float(_env("TREND_KEYWORD_SLEEP_SEC", "0.2")),
            request_timeout_sec=float(_env("TREND_REQUEST_TIMEOUT_SEC", "10")),
            max_retries=int(_env("TREND_MAX_RETRIES", "3")),
            mention_weight=float(_env("TREND_MENTION_WEIGHT", "5.0")),
            like_weight=float(_env("TREND_LIKE_WEIGHT", "1.0")),
            review_weight=float(_env("TREND_REVIEW_WEIGHT", "2.0")),
            rating_weight=float(_env("TREND_RATING_WEIGHT", "0.5")),
            mention_decay_factor=float(_env("TREND_MENTION_DECAY_FACTOR", "0.99")),
            popularity_top_rank_n=int(_env("TREND_POPULARITY_TOP_RANK_N", "10")),
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
