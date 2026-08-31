from __future__ import annotations

SMALL_TOWN_SIGNGU_CODES: dict[str, frozenset[str]] = {
    "51": frozenset({
        "190",  # 태백시
        "230",  # 삼척시
        "720",  # 홍천군
        "730",  # 횡성군
        "750",  # 영월군
        "760",  # 평창군
        "770",  # 정선군
        "780",  # 철원군
        "790",  # 화천군
        "800",  # 양구군
        "820",  # 고성군
        "830",  # 양양군
    }),
}


def is_small_town(ldong_regn_code: str | None, ldong_signgu_code: str | None) -> bool:
    """법정동 광역시도·시군구 코드가 소도시 목록에 있으면 True."""
    if ldong_regn_code is None or ldong_signgu_code is None:
        return False
    return ldong_signgu_code in SMALL_TOWN_SIGNGU_CODES.get(ldong_regn_code, frozenset())
