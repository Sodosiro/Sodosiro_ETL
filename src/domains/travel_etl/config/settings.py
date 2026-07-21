"""travel_etl 설정.

환경변수를 불변 설정 객체로 모은다 (GoF Singleton — `get_settings()` 로 단일 인스턴스).
컨테이너에서는 docker-compose 가 환경변수를 주입하고,
로컬 실행 시에는 프로젝트 루트 `.env` 를 읽어 os.environ 을 보충한다 (환경변수 우선).
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

# sodosiro-ETL 루트 (src/domains/travel_etl/config/settings.py 기준 4단계 상위)
_PROJECT_ROOT = Path(__file__).resolve().parents[4]


def _merge_env_file(path: Path) -> None:
    """`.env` 의 KEY=VALUE 를 os.environ 에 보충한다.

    비어있지 않은 기존 환경변수가 우선한다. docker-compose 가
    `${VAR:-}` 치환으로 빈 문자열을 주입하는 경우는 미설정으로 간주해
    `.env` 값으로 채운다.
    """
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not os.environ.get(key):
            os.environ[key] = value.strip()


def _env(key: str, default: str = "") -> str:
    return os.environ.get(key, default).strip()


def _build_db_url() -> str:
    """TRAVEL_DB_URL 이 있으면 그대로, 없으면 POSTGRESS_* 조각으로 조립한다."""
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
class TravelEtlSettings:
    """여행지 ETL 전역 설정 (불변)."""

    public_data_base_url: str
    public_data_api_key: str
    db_url: str
    content_type_id: str | None # 비우면 관광·문화·음식·쇼핑·레포츠 등 전체 유형
    ldong_regn_code: str         # 법정동 시도 코드 (51 = 강원특별자치도)
    page_size: int              # 목록 API numOfRows
    min_call_interval_sec: float
    detail_batch_limit: int     # 한 번의 실행에서 상세를 채울 최대 여행지 수
    image_recovery_batch_limit: int  # 이미지 복구 DAG 실행당 최대 contentId 수
    request_timeout_sec: float
    max_retries: int
    spring_base_url: str        # 적재 완료 후 변경분을 통지할 Spring 서버
    spring_api_username: str    # 비우면 인증 없이 호출
    spring_api_password: str
    spring_notify_limit: int    # 한 실행에서 Spring AI에 통지할 최대 건수
    spring_notify_batch: int
    spring_timeout_sec: float
    snapshot_dir: str = str(_PROJECT_ROOT / "data" / "travel" / "raw")

    @classmethod
    def from_env(cls) -> "TravelEtlSettings":
        return cls(
            public_data_base_url=_env("PUBLIC_DATA_BASE_URL"),
            public_data_api_key=_env("PUBLIC_DATA_API_KEY"),
            db_url=_build_db_url(),
            snapshot_dir=_env("TRAVEL_SNAPSHOT_DIR", str(_PROJECT_ROOT / "data" / "travel" / "raw")),
            content_type_id=_env("TRAVEL_CONTENT_TYPE_ID") or None,
            ldong_regn_code=_env("TRAVEL_LDONG_REGN_CODE", "51"),
            # areaBasedList2 스냅샷은 페이지당 500건으로 받아 호출 횟수를 줄인다.
            page_size=int(_env("PUBLIC_DATA_PAGE_SIZE", "500")),
            min_call_interval_sec=float(_env("API_MIN_INTERVAL_SEC", "0.5")),
            detail_batch_limit=int(_env("DETAIL_BATCH_LIMIT", "300")),
            image_recovery_batch_limit=int(_env("IMAGE_RECOVERY_BATCH_LIMIT", "500")),
            request_timeout_sec=float(_env("API_TIMEOUT_SEC", "15")),
            max_retries=int(_env("API_MAX_RETRIES", "3")),
            spring_base_url=_env("SPRING_BASE_URL"),
            spring_api_username=_env("SPRING_API_USERNAME"),
            spring_api_password=_env("SPRING_API_PASSWORD"),
            # 테스트 중 의도치 않은 대량 임베딩 비용을 막기 위한 안전한 기본값.
            spring_notify_limit=int(_env("SPRING_NOTIFY_LIMIT", "10")),
            spring_notify_batch=int(_env("SPRING_NOTIFY_BATCH", "500")),
            spring_timeout_sec=float(_env("SPRING_TIMEOUT_SEC", "10")),
        )


@lru_cache(maxsize=1)
def get_settings() -> TravelEtlSettings:
    env_file = _PROJECT_ROOT / ".env"
    if env_file.is_file():
        _merge_env_file(env_file)
    return TravelEtlSettings.from_env()
