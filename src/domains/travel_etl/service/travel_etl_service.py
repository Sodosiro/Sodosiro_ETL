"""여행지 ETL 업무 서비스.

Airflow DAG 태스크는 이 클래스의 메서드 하나씩만 호출한다.
클라이언트·정규화·저장소 조립은 내부에서 처리하며(Factory),
모든 외부 호출은 GuardedPublicDataClient(Proxy)를 거쳐 호출 간격을 지킨다.
간격 제한은 세션(태스크) 단위이므로 원천 API 를 부르는 태스크는
DAG 에서 직렬로 연결해야 합산 호출 속도가 유지된다.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import date
from typing import Iterator

from src.domains.travel_etl.client.public_data_client import (
    ApiClientBase,
    Endpoint,
    GuardedPublicDataClient,
    PublicDataClient,
    PublicDataQuotaError,
    PublicDataRateLimitError,
)
from src.domains.travel_etl.client.spring_client import SpringClient
from src.domains.travel_etl.config.settings import TravelEtlSettings, get_settings
from src.domains.travel_etl.controller.dto.models import StageStats
from src.domains.travel_etl.repository.travel_repository import ConnectionFactory, TravelRepository
from src.domains.travel_etl.service.normalizer import NormalizerFactory
from src.domains.travel_etl.service.rate_limiter import (
    MinIntervalLimiter,
)
from src.domains.travel_etl.service.snapshot_store import AreaBasedSnapshotStore

logger = logging.getLogger(__name__)


class TravelEtlService:
    """수집 → 정규화 → 적재 → 상세 보강 → 이미지 → 임베딩."""

    def __init__(self, settings: TravelEtlSettings | None = None) -> None:
        self._settings = settings or get_settings()
        self._connections = ConnectionFactory(self._settings.db_url)

    @contextmanager
    def _session(self) -> Iterator[tuple[TravelRepository, ApiClientBase]]:
        """저장소와 호출 간격 제한이 적용된 클라이언트를 함께 연다."""
        with self._connections.open() as repo:
            limiter = MinIntervalLimiter(self._settings.min_call_interval_sec)
            client = GuardedPublicDataClient(PublicDataClient(self._settings), limiter)
            yield repo, client

    # ── 태스크 진입점 ──────────────────────────────────────

    def sync_codes(self) -> dict:
        """법정동 코드(ldongCode2)와 신규 대분류 코드표(lclsSystmCode2)를 동기화한다.

        areaCode2 지역코드와 cat1~3 분류는 레거시 — 코드 테이블은
        법정동·신규 분류 체계만 담는다.
        """
        with self._session() as (repo, client):
            areas = self._sync_area_codes(repo, client)
            sigungus = self._sync_sigungu_codes(repo, client)
            categories = self._sync_categories(repo, client)
        return {"area_codes": areas, "sigungu_codes": sigungus, "categories": categories}

    def ensure_base_codes(self) -> dict:
        """스냅샷 수집의 전제인 시군구 코드가 없으면(신규 환경) 코드표를 동기화한다.

        코드표 전체 동기화는 API 호출이 많아 매일 돌리지 않고,
        `_fetch_spot_items` 가 조회하는 것과 같은 조건이 비어 있을 때만 수행한다.
        """
        with self._connections.open() as repo:
            bootstrapped = not repo.list_sigungu_codes(self._settings.ldong_regn_code)
        if not bootstrapped:
            return {"synced": False}
        logger.info("시군구 코드 없음 (%s) — 코드표 동기화 실행", self._settings.ldong_regn_code)
        return {"synced": True, **self.sync_codes()}

    def download_spot_snapshot(self, execution_date: date) -> dict:
        """논리 실행일 스냅샷을 재사용하거나 없을 때만 areaBasedList2를 수집한다."""
        store = AreaBasedSnapshotStore(self._settings.snapshot_dir)
        existing = store.path_for(execution_date)
        if existing.is_file():
            result = {"snapshot_path": str(existing), "snapshot_reused": True, "api_calls": 0}
            logger.info("관광지 원천 스냅샷 재사용: %s", result)
            return result
        with self._session() as (repo, client):
            items = self._fetch_spot_items(repo, client)
            path = store.write(items, execution_date)
            api_calls = client.calls_made
        result = {
            "snapshot_path": str(path),
            "snapshot_rows": len(items),
            "snapshot_reused": False,
            "api_calls": api_calls,
        }
        logger.info("관광지 원천 스냅샷 저장: %s", result)
        return result

    def load_spot_snapshot(self, snapshot_path: str) -> dict:
        """CSV 스냅샷을 정규화해 변경분만 UPSERT하고 후속 큐를 활성화한다."""
        stats = StageStats()
        with self._session() as (repo, client):
            items = AreaBasedSnapshotStore.read(snapshot_path)
            rows, quarantined = NormalizerFactory.tourist_spot().normalize_many(items)
            # 페이지 순회 중 원천 변동(정렬 시프트)으로 같은 content_id 가 중복 수집될 수
            # 있다 — 한 UPSERT 배치에 중복 키가 들어가면 statement 전체가 실패한다.
            deduped = self._dedup_by_content_id(rows)
            stats.extra["duplicated"] = len(rows) - len(deduped)
            rows = deduped
            category_count = self._sync_categories_if_missing(repo, client, rows)
            changed = self._filter_changed(repo, rows)
            repo.upsert_tourist_spots(changed)
            repo.upsert_spot_states(changed)
            stats.processed, stats.changed = len(rows), len(changed)
            stats.quarantined, stats.api_calls = len(quarantined), client.calls_made
            stats.extra["categories_synced"] = category_count
            stats.extra["snapshot_path"] = snapshot_path
        logger.info("관광지 스냅샷 적재: %s", stats.as_dict())
        return stats.as_dict()

    def enrich_detail_common(self) -> dict:
        return self._process_detail_pending("detail_common", self._apply_detail_common)

    def enrich_detail_intro(self) -> dict:
        return self._process_detail_pending("detail_intro", self._apply_detail_intro)

    def collect_detail_info(self) -> dict:
        return self._process_detail_pending("detail_info", self._apply_detail_info)

    def collect_images(self) -> dict:
        """변경분에 대해 detailImage2 로 이미지를 전체 교체한다."""
        stats = StageStats()
        with self._session() as (repo, client):
            pending = repo.fetch_pending("image", self._settings.detail_batch_limit)
            stats.extra["pending"] = len(pending)
            done = self._collect_images_each(repo, client, pending, stats)
            stats.processed, stats.api_calls = len(done), client.calls_made
        logger.info("이미지 수집: %s", stats.as_dict())
        return stats.as_dict()

    def recover_missing_images(self) -> dict:
        """이미지 행이 없는 콘텐츠를 재조회하고 원천 무이미지는 다음 실행에서 제외한다."""
        stats = StageStats()
        with self._session() as (repo, client):
            targets = repo.fetch_missing_image_content_ids(
                self._settings.image_recovery_batch_limit
            )
            stats.extra["targets"] = len(targets)
            done = self._collect_images_each(repo, client, targets, stats)
            stats.processed, stats.api_calls = len(done), client.calls_made
            stats.extra["limit"] = self._settings.image_recovery_batch_limit
        logger.info("누락 이미지 복구: %s", stats.as_dict())
        return stats.as_dict()

    def notify_spring(self, run_id: str) -> dict:
        """적재 완료 후 AI 처리 대상 content_id 를 Spring 에 통지한다.

        embed_pending 해제는 Spring 의 책임 — 통지가 실패해도 pending 이 큐로 남아
        다음 실행에서 다시 통지되므로 유실이 없다.
        """
        with self._connections.open() as repo:
            targets = repo.fetch_ready_for_ai(self._settings.spring_notify_limit)
        if not targets:
            return {"notified": 0}
        accepted = SpringClient(self._settings).notify_changed_spots(run_id, targets)
        logger.info(
            "Spring 통지: 대상 %d건(실행 한도 %d), 수락 %d건",
            len(targets), self._settings.spring_notify_limit, accepted,
        )
        return {
            "notified": len(targets),
            "accepted": accepted,
            "limit": self._settings.spring_notify_limit,
        }

    def start_run(self, run_id: str, dag_id: str) -> dict:
        """DAG 실행 이력만 시작한다. DDL은 배치 실행 경로에 포함하지 않는다."""
        with self._connections.open() as repo:
            repo.start_run(run_id, dag_id)
        return {"run_id": run_id}

    def finalize_run(self, run_id: str, dag_id: str, status: str, stats: dict) -> dict:
        with self._connections.open() as repo:
            repo.finish_run(run_id, dag_id, status, stats)
        return stats

    # ── 내부 단계 ─────────────────────────────────────────

    def _sync_area_codes(self, repo: TravelRepository, client: ApiClientBase) -> int:
        """법정동 시도 코드표 동기화 — ldongCode2 를 파라미터 없이 호출하면 시도 목록."""
        items = list(client.iter_items(Endpoint.LDONG_CODE, num_rows=50))
        rows, _ = NormalizerFactory.area_code().normalize_many(items)
        return repo.upsert_area_codes(rows)

    def _sync_sigungu_codes(self, repo: TravelRepository, client: ApiClientBase) -> int:
        """법정동 시군구 코드표 동기화 — 시도별 ldongCode2?lDongRegnCd 조회."""
        total = 0
        for area_code in repo.list_area_codes():
            items = list(
                client.iter_items(Endpoint.LDONG_CODE, num_rows=100, lDongRegnCd=area_code)
            )
            rows, _ = NormalizerFactory.sigungu_code(area_code).normalize_many(items)
            total += repo.upsert_sigungu_codes(rows)
        return total

    def _sync_categories(self, repo: TravelRepository, client: ApiClientBase) -> int:
        """신규 대분류(lclsSystm1~3) 코드표 전체를 계층 순서로 수집한다."""
        total = 0
        cat1_rows = self._fetch_categories(client, depth=1)
        total += repo.upsert_categories(cat1_rows)
        for cat1 in cat1_rows:
            cat2_rows = self._fetch_categories(client, depth=2, lclsSystm1=cat1.code)
            total += repo.upsert_categories(cat2_rows)
            for cat2 in cat2_rows:
                cat3_rows = self._fetch_categories(
                    client,
                    depth=3,
                    lclsSystm1=cat1.code,
                    lclsSystm2=cat2.code,
                )
                total += repo.upsert_categories(cat3_rows)
        return total

    def _fetch_categories(self, client: ApiClientBase, depth: int, **params):
        parent = params.get("lclsSystm2") or params.get("lclsSystm1")
        items = list(client.iter_items(Endpoint.LCLS_SYSTM_CODE, num_rows=100, **params))
        rows, _ = NormalizerFactory.category(depth, parent).normalize_many(items)
        return rows

    def _sync_categories_if_missing(self, repo, client, rows) -> int:
        """수집 데이터가 쓰는 분류 코드가 없을 때만 코드표를 다시 조회한다."""
        required = {
            code
            for row in rows
            for code in (row.lcls_systm1, row.lcls_systm2, row.lcls_systm3)
            if code
        }
        if required.issubset(repo.list_category_codes()):
            return 0
        return self._sync_categories(repo, client)

    def _fetch_spot_items(self, repo: TravelRepository, client: ApiClientBase) -> list[dict]:
        """법정동 코드별 전체 콘텐츠를 수집한다 (contentTypeId가 비면 전체 유형)."""
        signgu_codes = repo.list_sigungu_codes(self._settings.ldong_regn_code)
        if not signgu_codes:
            raise RuntimeError(
                f"법정동 시군구 코드가 DB에 없습니다: {self._settings.ldong_regn_code}"
            )
        items: list[dict] = []
        for signgu_code in signgu_codes:
            items.extend(
                client.iter_items(
                    Endpoint.AREA_BASED_LIST,
                    num_rows=self._settings.page_size,
                    contentTypeId=self._settings.content_type_id,
                    lDongRegnCd=self._settings.ldong_regn_code,
                    lDongSignguCd=signgu_code,
                )
            )
        return items

    def _filter_changed(self, repo: TravelRepository, rows) -> list:
        """신규이거나 content_hash 가 달라진 행만 남긴다 (중복 적재 방지)."""
        existing = repo.load_spot_hashes()
        return [r for r in rows if existing.get(r.content_id) != r.content_hash]

    @staticmethod
    def _dedup_by_content_id(rows: list) -> list:
        """content_id 기준으로 중복을 제거한다 (뒤에 수집된 행 우선)."""
        return list({row.content_id: row for row in rows}.values())

    def _process_detail_pending(self, kind: str, processor) -> dict:
        """상세 API 하나의 pending 큐를 지정된 배치만큼 처리한다."""
        stats = StageStats()
        with self._session() as (repo, client):
            pending = repo.fetch_pending(kind, self._settings.detail_batch_limit)
            stats.extra["pending"] = len(pending)
            done = self._process_each(repo, client, kind, pending, stats, processor)
            stats.processed, stats.api_calls = len(done), client.calls_made
            stats.extra["limit"] = self._settings.detail_batch_limit
        logger.info("상세 보강(%s): %s", kind, stats.as_dict())
        return stats.as_dict()

    def _process_each(self, repo, client, kind: str, pending: list[int], stats: StageStats, processor) -> list[int]:
        """항목마다 독립 트랜잭션으로 처리한다.

        - 성공: pending 해제 후 즉시 커밋 — 잠금 보유 시간을 항목 단위로 줄인다.
        - 쿼터/속도 제한: 재시도가 무의미하므로 중단하고 나머지는 다음 실행에 넘긴다.
        - 그 외 예외: 해당 항목만 롤백·기록하고 계속 진행한다 (poison-pill 방지).
        """
        done: list[int] = []
        for content_id in pending:
            try:
                processor(repo, client, content_id)
            except (PublicDataQuotaError, PublicDataRateLimitError) as e:
                repo.rollback()
                stats.stopped_reason = str(e)
                break
            except Exception:
                repo.rollback()
                stats.failed += 1
                logger.exception("항목 처리 실패 — 건너뜀 (kind=%s, content_id=%s)", kind, content_id)
                continue
            repo.clear_pending(kind, [content_id])
            repo.commit()
            done.append(content_id)
        return done

    @staticmethod
    def _apply_detail_common(repo, client, content_id: int) -> None:
        common_item = client.get_single(Endpoint.DETAIL_COMMON, contentId=content_id)
        if common_item:
            row = NormalizerFactory.detail_common().normalize(common_item)
            if row:
                repo.apply_detail_common(row)

    @staticmethod
    def _apply_detail_intro(repo, client, content_id: int) -> None:
        content_type_id = repo.find_content_type_id(content_id)
        intro_item = (
            client.get_single(
                Endpoint.DETAIL_INTRO,
                contentId=content_id,
                contentTypeId=content_type_id,
            )
            if content_type_id
            else None
        )
        if intro_item:
            row = NormalizerFactory.detail_intro().normalize(intro_item)
            if row:
                repo.apply_detail_intro(row)

    @staticmethod
    def _apply_detail_info(repo, client, content_id: int) -> None:
        content_type_id = repo.find_content_type_id(content_id)
        if not content_type_id:
            return
        items = list(
            client.iter_items(
                Endpoint.DETAIL_INFO,
                num_rows=100,
                contentId=content_id,
                contentTypeId=content_type_id,
            )
        )
        repo.update_detail_info(content_id, items)

    def _collect_images_each(self, repo, client, pending: list[int], stats: StageStats) -> list[int]:
        """이미지 수집도 `_process_each` 와 동일한 항목 단위 트랜잭션 규칙을 따른다."""
        return self._process_each(repo, client, "image", pending, stats, self._apply_images)

    @staticmethod
    def _apply_images(repo, client, content_id: int) -> None:
        items = list(
            client.iter_items(
                Endpoint.DETAIL_IMAGE, num_rows=100,
                contentId=content_id, imageYN="Y",
            )
        )
        rows, _ = NormalizerFactory.spot_image(content_id).normalize_many(items)
        repo.replace_spot_images(content_id, rows)
        if rows and rows[0].image_url:
            repo.update_first_image_if_missing(content_id, rows[0].image_url)
        repo.mark_image_absent(content_id, absent=not rows)
