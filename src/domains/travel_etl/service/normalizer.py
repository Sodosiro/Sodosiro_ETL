"""원천 item(dict) → 정규화 행 변환.

적용 패턴
- Strategy: 엔드포인트별 Normalizer 가 동일 인터페이스(`normalize`)를 구현한다.
- Adapter: 공공데이터 필드명(contentid, mapx …)을 업무 스키마 필드로 변환한다.
- Factory Method: `NormalizerFactory` 가 엔드포인트에 맞는 전략을 생성한다.

검증 실패 레코드는 예외로 전체를 멈추지 않고 격리 목록으로 반환한다.
"""
from __future__ import annotations

import hashlib
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Generic, TypeVar

from src.domains.travel_etl.controller.dto.models import (
    AreaCodeRow,
    CategoryRow,
    DetailCommonRow,
    DetailIntroRow,
    SigunguCodeRow,
    SpotImageRow,
    TouristSpotRow,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")

# 대한민국 좌표 유효 범위 (벗어나면 좌표만 버린다)
_LON_RANGE = (Decimal("124"), Decimal("132"))
_LAT_RANGE = (Decimal("33"), Decimal("39"))
_CONTENT_HASH_VERSION = "v2-lcls-columns"


def _text(item: dict, key: str) -> str | None:
    """공백·빈 문자열을 None 으로 정리한 문자열 필드."""
    value = item.get(key)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _decimal(item: dict, key: str) -> Decimal | None:
    raw = _text(item, key)
    if raw is None:
        return None
    try:
        return Decimal(raw)
    except InvalidOperation:
        return None


def _int(item: dict, key: str) -> int | None:
    raw = _text(item, key)
    return int(raw) if raw and raw.isdigit() else None


def _datetime14(item: dict, key: str) -> datetime | None:
    """YYYYMMDDHHMMSS 형식 시각."""
    raw = _text(item, key)
    if not raw:
        return None
    try:
        return datetime.strptime(raw[:14].ljust(14, "0"), "%Y%m%d%H%M%S")
    except ValueError:
        return None


def _in_range(value: Decimal | None, bounds: tuple[Decimal, Decimal]) -> Decimal | None:
    if value is None or not bounds[0] <= value <= bounds[1]:
        return None
    return value


def compute_content_hash(item: dict) -> str:
    """목록 응답의 검색 대상 필드로 변경 감지 해시를 만든다 (modifiedtime 포함)."""
    keys = (
        "contentid", "contenttypeid", "title", "addr1", "addr2", "zipcode", "mapx", "mapy",
        "mlevel", "lDongRegnCd", "lDongSignguCd", "areacode", "sigungucode",
        "lclsSystm1", "lclsSystm2", "lclsSystm3", "cat1", "cat2", "cat3",
        "firstimage", "modifiedtime",
    )
    # 정규화/스키마 의미가 바뀌면 버전을 올려 기존 행을 한 번 안전하게 재적재한다.
    joined = _CONTENT_HASH_VERSION + "|" + "|".join(str(item.get(k) or "") for k in keys)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


class Normalizer(ABC, Generic[T]):
    """엔드포인트별 정규화 전략 (Strategy)."""

    @abstractmethod
    def normalize(self, item: dict) -> T | None:
        """유효하면 행을, 검증 실패면 None 을 반환한다."""

    def normalize_many(self, items: list[dict]) -> tuple[list[T], list[dict]]:
        """일괄 정규화 — (정상 행, 격리 대상 원본) 을 반환한다."""
        rows: list[T] = []
        quarantined: list[dict] = []
        for item in items:
            row = self.normalize(item)
            (rows.append(row) if row is not None else quarantined.append(item))
        return rows, quarantined


class AreaCodeNormalizer(Normalizer[AreaCodeRow]):
    def normalize(self, item: dict) -> AreaCodeRow | None:
        code, name = _text(item, "code"), _text(item, "name")
        return AreaCodeRow(code, name) if code and name else None


class SigunguCodeNormalizer(Normalizer[SigunguCodeRow]):
    def __init__(self, area_code: str) -> None:
        self._area_code = area_code

    def normalize(self, item: dict) -> SigunguCodeRow | None:
        code, name = _text(item, "code"), _text(item, "name")
        return SigunguCodeRow(self._area_code, code, name) if code and name else None


class CategoryNormalizer(Normalizer[CategoryRow]):
    def __init__(self, depth: int, parent_code: str | None) -> None:
        self._depth = depth
        self._parent_code = parent_code

    def normalize(self, item: dict) -> CategoryRow | None:
        code, name = _text(item, "code"), _text(item, "name")
        if not code or not name:
            return None
        return CategoryRow(code, name, self._depth, self._parent_code)


class TouristSpotNormalizer(Normalizer[TouristSpotRow]):
    """areaBasedList2 item → tourist_spot 행. contentid·title 없으면 격리."""

    def normalize(self, item: dict) -> TouristSpotRow | None:
        content_id = _int(item, "contentid")
        title = _text(item, "title")
        if content_id is None or title is None:
            logger.warning("필수값 누락으로 격리: contentid=%s", item.get("contentid"))
            return None
        return TouristSpotRow(
            content_id=content_id,
            content_type_id=_text(item, "contenttypeid"),
            title=title[:200],
            addr1=_text(item, "addr1"),
            addr2=_text(item, "addr2"),
            zipcode=_text(item, "zipcode"),
            map_x=_in_range(_decimal(item, "mapx"), _LON_RANGE),
            map_y=_in_range(_decimal(item, "mapy"), _LAT_RANGE),
            map_level=_int(item, "mlevel"),
            ldong_regn_code=_text(item, "lDongRegnCd"),
            ldong_signgu_code=_text(item, "lDongSignguCd"),
            area_code=_text(item, "areacode"),
            sigungu_code=_text(item, "sigungucode"),
            cat1=_text(item, "cat1"),
            cat2=_text(item, "cat2"),
            cat3=_text(item, "cat3"),
            lcls_systm1=_text(item, "lclsSystm1"),
            lcls_systm2=_text(item, "lclsSystm2"),
            lcls_systm3=_text(item, "lclsSystm3"),
            first_image=_text(item, "firstimage"),
            created_time=_datetime14(item, "createdtime"),
            content_hash=compute_content_hash(item),
        )


class SpotImageNormalizer(Normalizer[SpotImageRow]):
    """detailImage2 item → spot_image 행. order 는 응답 순서로 부여한다."""

    def __init__(self, content_id: int) -> None:
        self._content_id = content_id
        self._order = 0

    def normalize(self, item: dict) -> SpotImageRow | None:
        url = _text(item, "originimgurl") or _text(item, "smallimageurl")
        if url is None:
            return None
        self._order += 1
        return SpotImageRow(
            content_id=self._content_id,
            order=self._order,
            type=_text(item, "imgname"),
            image_url=url,
        )


class DetailCommonNormalizer(Normalizer[DetailCommonRow]):
    def normalize(self, item: dict) -> DetailCommonRow | None:
        content_id = _int(item, "contentid")
        if content_id is None:
            return None
        return DetailCommonRow(
            content_id=content_id,
            homepage=_text(item, "homepage"),
            overview=_text(item, "overview"),
        )


class DetailIntroNormalizer(Normalizer[DetailIntroRow]):
    def normalize(self, item: dict) -> DetailIntroRow | None:
        content_id = _int(item, "contentid")
        if content_id is None:
            return None
        infocenter = _text(item, "infocenter")
        return DetailIntroRow(
            content_id=content_id,
            infocenter=infocenter[:200] if infocenter else None,
            usetime=_text(item, "usetime"),
            restdate=_text(item, "restdate"),
            parking=_text(item, "parking"),
            expguide=_text(item, "expguide"),
        )


class NormalizerFactory:
    """정규화 전략 Factory Method."""

    @staticmethod
    def area_code() -> AreaCodeNormalizer:
        return AreaCodeNormalizer()

    @staticmethod
    def sigungu_code(area_code: str) -> SigunguCodeNormalizer:
        return SigunguCodeNormalizer(area_code)

    @staticmethod
    def category(depth: int, parent_code: str | None = None) -> CategoryNormalizer:
        return CategoryNormalizer(depth, parent_code)

    @staticmethod
    def tourist_spot() -> TouristSpotNormalizer:
        return TouristSpotNormalizer()

    @staticmethod
    def spot_image(content_id: int) -> SpotImageNormalizer:
        return SpotImageNormalizer(content_id)

    @staticmethod
    def detail_common() -> DetailCommonNormalizer:
        return DetailCommonNormalizer()

    @staticmethod
    def detail_intro() -> DetailIntroNormalizer:
        return DetailIntroNormalizer()
