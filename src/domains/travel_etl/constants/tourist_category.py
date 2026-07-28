"""TourAPI 신규 분류 코드를 서비스용 7개 관광 카테고리로 변환한다."""
from __future__ import annotations

from enum import IntEnum


class TouristCategory(IntEnum):
    RESTAURANT = 1
    CAFE = 2
    SHOPPING = 3
    TOURIST_ATTRACTION = 4
    NATURE = 5
    ACTIVITY = 6
    ACCOMMODATION = 7


def classify_tourist_category(lcls_systm1: str | None, lcls_systm2: str | None) -> int | None:
    """TourAPI 대·중분류를 서비스 카테고리 정수로 변환한다.

    음식(FD)은 중분류 ``FD05``(카페/찻집)만 카페로 분리한다. 나머지 음식은
    식당이며, 역사·문화·코스·행사는 관광지로 통합한다.
    """
    if lcls_systm1 == "FD":
        return TouristCategory.CAFE if lcls_systm2 == "FD05" else TouristCategory.RESTAURANT
    if lcls_systm1 == "SH":
        return TouristCategory.SHOPPING
    if lcls_systm1 in {"VE", "HS", "C01", "EV"}:
        return TouristCategory.TOURIST_ATTRACTION
    if lcls_systm1 == "NA":
        return TouristCategory.NATURE
    if lcls_systm1 in {"EX", "LS"}:
        return TouristCategory.ACTIVITY
    if lcls_systm1 == "AC":
        return TouristCategory.ACCOMMODATION
    return None
