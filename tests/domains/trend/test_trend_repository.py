import unittest

from src.domains.trend.repository.trend_repository import (
    KakaoSpotRow,
    _merge_kakao_spot_rows,
)


def _row(place_id: str, blog: int, cafe: int, score: float) -> KakaoSpotRow:
    return KakaoSpotRow(
        kakao_place_id=place_id,
        place_name="춘천 닭갈비",
        city_name="춘천시",
        address_name="강원특별자치도 춘천시",
        category_group_code="FD6",
        category_group_name="음식점",
        longitude=127.7,
        latitude=37.8,
        blog_mention_count=blog,
        cafe_mention_count=cafe,
        popularity_score=score,
    )


class MergeKakaoSpotRowsTest(unittest.TestCase):
    def test_merges_duplicate_place_id_and_sums_metrics(self) -> None:
        merged = _merge_kakao_spot_rows(
            [_row("same-place", 3, 2, 1.5), _row("same-place", 4, 1, 2.0)]
        )

        self.assertEqual(len(merged), 1)
        self.assertEqual(merged[0].blog_mention_count, 7)
        self.assertEqual(merged[0].cafe_mention_count, 3)
        self.assertEqual(merged[0].popularity_score, 3.5)

    def test_preserves_distinct_places(self) -> None:
        merged = _merge_kakao_spot_rows([_row("place-1", 1, 0, 0.5), _row("place-2", 2, 1, 1.0)])

        self.assertEqual([row.kakao_place_id for row in merged], ["place-1", "place-2"])


if __name__ == "__main__":
    unittest.main()
