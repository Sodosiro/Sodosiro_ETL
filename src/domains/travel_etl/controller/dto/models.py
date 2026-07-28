"""정규화된 행 모델.

공공데이터 원천 item(dict) 을 업무 DB 테이블 행으로 변환한 결과.
테이블·컬럼 구조는 sodosiro-BE 의 travel 엔티티와 1:1 로 맞춘다.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class AreaCodeRow:
    """area_code — 광역시도 (areaCode2)."""

    area_code: str
    name: str


@dataclass(frozen=True)
class SigunguCodeRow:
    """sigungu_code — 시군구 (areaCode2?areaCode=X)."""

    area_code: str
    sigungu_code: str
    name: str


@dataclass(frozen=True)
class CategoryRow:
    """category — 신규 대/중/소 분류 계층 (lclsSystmCode2)."""

    code: str
    name: str
    depth: int
    parent_code: str | None


@dataclass(frozen=True)
class TouristSpotRow:
    """tourist_spot — 여행 콘텐츠 기본정보 (areaBasedList2).

    content_hash 는 DB 컬럼이 아니라 ETL 메타(etl_spot_state)에 저장하는 변경 감지 키.
    """

    content_id: int
    content_type_id: str | None
    title: str
    addr1: str | None
    addr2: str | None
    zipcode: str | None
    map_x: Decimal | None
    map_y: Decimal | None
    map_level: int | None
    ldong_regn_code: str | None
    ldong_signgu_code: str | None
    sigungu_code: str | None
    category: int
    first_image: str | None
    created_time: datetime | None
    content_hash: str


@dataclass(frozen=True)
class SpotImageRow:
    """spot_image — 관광지 이미지 (detailImage2)."""

    content_id: int
    order: int
    image_url: str | None


@dataclass(frozen=True)
class DetailCommonRow:
    """detailCommon2 결과 중 적재 대상 필드."""

    content_id: int
    homepage: str | None
    overview: str | None


@dataclass(frozen=True)
class DetailIntroRow:
    """detailIntro2 결과 중 공통 스키마에 적재 가능한 필드."""

    content_id: int
    infocenter: str | None
    usetime: str | None
    restdate: str | None
    parking: str | None
    expguide: str | None


@dataclass
class StageStats:
    """태스크(스테이지) 단위 처리 통계 — finalize 에서 etl_run.stats 로 집계."""

    processed: int = 0
    changed: int = 0
    quarantined: int = 0
    failed: int = 0  # 항목 단위 처리 실패 — pending 에 남아 다음 실행에서 재시도된다
    api_calls: int = 0
    stopped_reason: str | None = None
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        data = {
            "processed": self.processed,
            "changed": self.changed,
            "quarantined": self.quarantined,
            "failed": self.failed,
            "api_calls": self.api_calls,
        }
        if self.stopped_reason:
            data["stopped_reason"] = self.stopped_reason
        data.update(self.extra)
        return data
