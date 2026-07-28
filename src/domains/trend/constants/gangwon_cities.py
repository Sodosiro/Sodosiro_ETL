"""강원도 18개 시·군 목록과 중심 좌표, 지역명 필터 집합."""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class GangwonCity:
    name: str        # 풀네임 (예: 춘천시)
    short_name: str  # 단축명 — API 쿼리에 사용 (예: 춘천)
    longitude: float  # 카카오맵 x
    latitude: float   # 카카오맵 y


GANGWON_CITIES: tuple[GangwonCity, ...] = (
    GangwonCity("춘천시",  "춘천",  127.7292, 37.8748),
    GangwonCity("원주시",  "원주",  127.9247, 37.3422),
    GangwonCity("강릉시",  "강릉",  128.8781, 37.7519),
    GangwonCity("동해시",  "동해",  129.1139, 37.5247),
    GangwonCity("태백시",  "태백",  128.9856, 37.1640),
    GangwonCity("속초시",  "속초",  128.5919, 38.2070),
    GangwonCity("삼척시",  "삼척",  129.1650, 37.4494),
    GangwonCity("홍천군",  "홍천",  127.8879, 37.6975),
    GangwonCity("횡성군",  "횡성",  127.9851, 37.4916),
    GangwonCity("영월군",  "영월",  128.4614, 37.1838),
    GangwonCity("평창군",  "평창",  128.3890, 37.3700),
    GangwonCity("정선군",  "정선",  128.6606, 37.3800),
    GangwonCity("철원군",  "철원",  127.3130, 38.1467),
    GangwonCity("화천군",  "화천",  127.7078, 38.1064),
    GangwonCity("양구군",  "양구",  128.0254, 38.1097),
    GangwonCity("인제군",  "인제",  128.1706, 38.0694),
    GangwonCity("고성군",  "고성",  128.4677, 38.3806),
    GangwonCity("양양군",  "양양",  128.6192, 38.0784),
)

# 형태소 분석 결과에서 exact match 로 제거할 지역명 집합
GANGWON_REGION_NAMES: frozenset[str] = frozenset(
    {name for city in GANGWON_CITIES for name in (city.name, city.short_name)}
    | {"강원도", "강원", "강원특별자치도"}
)
