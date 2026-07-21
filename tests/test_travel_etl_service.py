"""High 리뷰 수정 검증 — 항목 단위 예외 격리(H-1/H-6), 배치 dedup(H-2), 부트스트랩(H-5)."""
from __future__ import annotations

import unittest
from dataclasses import dataclass
from unittest.mock import MagicMock

from src.domains.travel_etl.client.public_data_client import (
    Endpoint,
    PublicDataQuotaError,
    PublicDataRateLimitError,
)
from src.domains.travel_etl.controller.dto.models import StageStats
from src.domains.travel_etl.service.travel_etl_service import TravelEtlService

from tests._fixtures import FakeClient, FakeConnectionFactory, FakeRepo, make_settings


def make_service() -> TravelEtlService:
    return TravelEtlService(settings=make_settings())


class ProcessEachTest(unittest.TestCase):
    """`_process_each` — pending 큐 처리의 예외/트랜잭션 규칙."""

    def setUp(self) -> None:
        self.service = make_service()
        self.repo = FakeRepo()
        self.stats = StageStats()

    def test_성공_항목마다_pending_해제와_커밋(self) -> None:
        done = self.service._process_each(
            self.repo, None, "detail_common", [1, 2], self.stats, lambda r, c, i: None
        )
        self.assertEqual(done, [1, 2])
        self.assertEqual(
            self.repo.calls,
            [
                ("clear_pending", "detail_common", [1]), ("commit",),
                ("clear_pending", "detail_common", [2]), ("commit",),
            ],
        )
        self.assertEqual(self.stats.failed, 0)
        self.assertIsNone(self.stats.stopped_reason)

    def test_일반_예외는_해당_항목만_롤백하고_계속_진행한다(self) -> None:
        """poison-pill 방지 — 문제 항목이 뒤 항목의 처리를 막지 않는다."""

        def processor(repo, client, content_id):
            if content_id == 2:
                raise ValueError("응답 스키마 변화")

        done = self.service._process_each(
            self.repo, None, "detail_common", [1, 2, 3], self.stats, processor
        )
        self.assertEqual(done, [1, 3])
        self.assertEqual(self.stats.failed, 1)
        self.assertIn(("rollback",), self.repo.calls)
        # 실패 항목(2)은 pending 해제 대상이 아니다 — 다음 실행에서 재시도된다.
        cleared = [c[2] for c in self.repo.calls if c[0] == "clear_pending"]
        self.assertEqual(cleared, [[1], [3]])

    def test_쿼터_초과는_롤백_후_중단하고_사유를_남긴다(self) -> None:
        def processor(repo, client, content_id):
            if content_id == 2:
                raise PublicDataQuotaError("22", "한도 초과")

        done = self.service._process_each(
            self.repo, None, "image", [1, 2, 3], self.stats, processor
        )
        self.assertEqual(done, [1])
        self.assertIsNotNone(self.stats.stopped_reason)
        self.assertEqual(self.stats.failed, 0)
        # 항목 3은 호출되지 않아야 한다 (쿼터 보호).
        self.assertEqual(self.repo.calls[-1], ("rollback",))

    def test_속도_제한도_중단_사유다(self) -> None:
        def processor(repo, client, content_id):
            raise PublicDataRateLimitError("429", "요청 속도 제한")

        done = self.service._process_each(
            self.repo, None, "image", [1, 2], self.stats, processor
        )
        self.assertEqual(done, [])
        self.assertIn("429", self.stats.stopped_reason)


class DedupTest(unittest.TestCase):
    """`_dedup_by_content_id` — 한 배치 안의 중복 키 제거 (UPSERT 실패 방지)."""

    def test_같은_content_id는_뒤에_수집된_행이_남는다(self) -> None:
        @dataclass
        class Row:
            content_id: int
            title: str

        rows = [Row(1, "a"), Row(2, "b"), Row(1, "a-갱신")]
        deduped = TravelEtlService._dedup_by_content_id(rows)
        self.assertEqual([(r.content_id, r.title) for r in deduped], [(1, "a-갱신"), (2, "b")])

    def test_중복이_없으면_그대로다(self) -> None:
        @dataclass
        class Row:
            content_id: int

        rows = [Row(3), Row(1), Row(2)]
        self.assertEqual(TravelEtlService._dedup_by_content_id(rows), rows)


class EnsureBaseCodesTest(unittest.TestCase):
    """`ensure_base_codes` — 신규 환경에서만 코드표를 동기화한다."""

    def test_시군구_코드가_있으면_API_호출_없이_통과한다(self) -> None:
        service = make_service()
        service._connections = FakeConnectionFactory(FakeRepo(sigungu_codes=["110"]))
        service.sync_codes = MagicMock()
        self.assertEqual(service.ensure_base_codes(), {"synced": False})
        service.sync_codes.assert_not_called()

    def test_시군구_코드가_없으면_전체_코드표를_동기화한다(self) -> None:
        service = make_service()
        service._connections = FakeConnectionFactory(FakeRepo(sigungu_codes=[]))
        service.sync_codes = MagicMock(
            return_value={"area_codes": 17, "sigungu_codes": 200, "categories": 300}
        )
        result = service.ensure_base_codes()
        self.assertTrue(result["synced"])
        self.assertEqual(result["sigungu_codes"], 200)
        service.sync_codes.assert_called_once()


class LdongCodeSyncTest(unittest.TestCase):
    """코드표 동기화 — areaCode2 레거시 대신 법정동(ldongCode2) 체계를 채운다."""

    def test_시도_코드는_ldongCode2_시도_목록으로_채운다(self) -> None:
        service = make_service()
        repo = FakeRepo()
        client = FakeClient({
            (Endpoint.LDONG_CODE, ()): [
                {"code": "11", "name": "서울특별시"},
                {"code": "51", "name": "강원특별자치도"},
            ],
        })
        total = service._sync_area_codes(repo, client)
        self.assertEqual(total, 2)
        area_rows = next(c[1] for c in repo.calls if c[0] == "upsert_area_codes")
        self.assertEqual([r.area_code for r in area_rows], ["11", "51"])
        # 레거시 areaCode2 엔드포인트는 호출하지 않는다.
        self.assertTrue(all(e is Endpoint.LDONG_CODE for e, _ in client.requests))

    def test_시군구_코드는_시도별_ldongCode2로_채운다(self) -> None:
        service = make_service()
        repo = FakeRepo(area_codes=["51"])
        client = FakeClient({
            (Endpoint.LDONG_CODE, (("lDongRegnCd", "51"),)): [
                {"code": "110", "name": "춘천시"},
                {"code": "130", "name": "원주시"},
            ],
        })
        total = service._sync_sigungu_codes(repo, client)
        self.assertEqual(total, 2)
        sigungu_rows = next(c[1] for c in repo.calls if c[0] == "upsert_sigungu_codes")
        self.assertEqual(
            [(r.area_code, r.sigungu_code) for r in sigungu_rows],
            [("51", "110"), ("51", "130")],
        )
        self.assertTrue(all(e is Endpoint.LDONG_CODE for e, _ in client.requests))


class StageStatsTest(unittest.TestCase):
    def test_failed_카운트가_stats에_포함된다(self) -> None:
        stats = StageStats(processed=2, failed=1)
        self.assertEqual(stats.as_dict()["failed"], 1)


if __name__ == "__main__":
    unittest.main()
