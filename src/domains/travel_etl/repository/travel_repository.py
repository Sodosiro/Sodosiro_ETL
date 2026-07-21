"""업무 DB 접근 계층 (Repository).

- 모든 적재는 자연키 기준 UPSERT(ON CONFLICT)로 멱등 — 재실행해도 두 번 적재되지 않는다.
- 대량 적재는 execute_values 배치로 처리해 왕복을 최소화한다.
- 테이블·컬럼은 sodosiro-BE JPA 엔티티(tourist_spot 등)와 동일하게 맞춘다.
- etl_* 테이블은 ETL 전용 메타(변경 해시, pending 상태, 호출 카운터, 실행 이력)다.
"""
from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from typing import Iterator

import psycopg2
import psycopg2.extras

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

_PENDING_COLUMNS = {
    "detail_common": "detail_common_pending",
    "detail_intro": "detail_intro_pending",
    "detail_info": "detail_info_pending",
    "image": "image_pending",
    "embed": "embed_pending",
}


class ConnectionFactory:
    """DB 커넥션 생성 책임 분리 (Factory)."""

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url

    @contextmanager
    def open(self) -> Iterator["TravelRepository"]:
        conn = psycopg2.connect(self._db_url)
        try:
            yield TravelRepository(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


class TravelRepository:
    """여행지 업무 DB + ETL 메타 저장소."""

    def __init__(self, conn) -> None:
        self._conn = conn

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    # ── 코드 테이블 UPSERT ──────────────────────────────────

    def upsert_area_codes(self, rows: list[AreaCodeRow]) -> int:
        return self._upsert(
            """INSERT INTO area_code (area_code, name) VALUES %s
               ON CONFLICT (area_code) DO UPDATE SET name = EXCLUDED.name""",
            [(r.area_code, r.name) for r in rows],
        )

    def list_area_codes(self) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT area_code FROM area_code ORDER BY area_code")
            return [row[0] for row in cur.fetchall()]

    def upsert_sigungu_codes(self, rows: list[SigunguCodeRow]) -> int:
        return self._upsert(
            """INSERT INTO sigungu_code (area_code, sigungu_code, name) VALUES %s
               ON CONFLICT (area_code, sigungu_code) DO UPDATE SET name = EXCLUDED.name""",
            [(r.area_code, r.sigungu_code, r.name) for r in rows],
        )

    def list_sigungu_codes(self, area_code: str) -> list[str]:
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT sigungu_code FROM sigungu_code
                   WHERE area_code = %s ORDER BY sigungu_code""",
                (area_code,),
            )
            return [row[0] for row in cur.fetchall()]

    def upsert_categories(self, rows: list[CategoryRow]) -> int:
        return self._upsert(
            """INSERT INTO category (code, name, depth, parent_code) VALUES %s
               ON CONFLICT (code) DO UPDATE
               SET name = EXCLUDED.name, depth = EXCLUDED.depth,
                   parent_code = EXCLUDED.parent_code""",
            [(r.code, r.name, r.depth, r.parent_code) for r in rows],
        )

    def list_category_codes(self) -> set[str]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT code FROM category")
            return {row[0] for row in cur.fetchall()}

    # ── 관광지 UPSERT + 변경 감지 ───────────────────────────

    def load_spot_hashes(self) -> dict[int, str]:
        """업무 행이 실제 존재하는 content_hash 맵 — 누락 행도 변경분으로 복구한다."""
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT state.content_id, state.content_hash
                   FROM etl_spot_state state
                   INNER JOIN tourist_spot spot ON spot.content_id = state.content_id"""
            )
            return dict(cur.fetchall())

    def upsert_tourist_spots(self, rows: list[TouristSpotRow]) -> int:
        return self._upsert(
            """INSERT INTO tourist_spot (
                   content_id, content_type_id, title, addr1, addr2, zipcode, map_x, map_y, map_level,
                   ldong_regn_code, ldong_signgu_code,
                   area_code, sigungu_code, cat1, cat2, cat3,
                   lcls_systm1, lcls_systm2, lcls_systm3, first_image,
                   created_time, collected_at
               ) VALUES %s
               ON CONFLICT (content_id) DO UPDATE SET
                   content_type_id = EXCLUDED.content_type_id,
                   title = EXCLUDED.title, addr1 = EXCLUDED.addr1, addr2 = EXCLUDED.addr2,
                   zipcode = EXCLUDED.zipcode, map_x = EXCLUDED.map_x, map_y = EXCLUDED.map_y,
                   map_level = EXCLUDED.map_level, area_code = EXCLUDED.area_code,
                   ldong_regn_code = EXCLUDED.ldong_regn_code,
                   ldong_signgu_code = EXCLUDED.ldong_signgu_code,
                   sigungu_code = EXCLUDED.sigungu_code, cat1 = EXCLUDED.cat1,
                   cat2 = EXCLUDED.cat2, cat3 = EXCLUDED.cat3,
                   lcls_systm1 = EXCLUDED.lcls_systm1,
                   lcls_systm2 = EXCLUDED.lcls_systm2,
                   lcls_systm3 = EXCLUDED.lcls_systm3,
                   first_image = EXCLUDED.first_image, created_time = EXCLUDED.created_time,
                   collected_at = EXCLUDED.collected_at""",
            [
                (
                    r.content_id, r.content_type_id, r.title, r.addr1, r.addr2, r.zipcode, r.map_x, r.map_y,
                    r.map_level, r.ldong_regn_code, r.ldong_signgu_code,
                    r.area_code, r.sigungu_code, r.cat1, r.cat2, r.cat3,
                    r.lcls_systm1, r.lcls_systm2, r.lcls_systm3,
                    r.first_image, r.created_time,
                )
                for r in rows
            ],
            template=(
                "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())"
            ),
        )

    def find_content_type_id(self, content_id: int) -> str | None:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT content_type_id FROM tourist_spot WHERE content_id = %s",
                (content_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None

    def upsert_spot_states(self, rows: list[TouristSpotRow]) -> int:
        """변경된 여행지의 해시와 CSV 대표 이미지 기준의 이미지 상태를 저장한다.

        ``firstimage`` 가 비어 있으면 이미지 원천 부재로 확정해 상세 이미지
        수집 큐에 넣지 않는다. 값이 있으면 이미지 수집 대상으로 다시 등록한다.
        """
        return self._upsert(
            """INSERT INTO etl_spot_state (
                   content_id, content_hash, image_pending, image_absent
               ) VALUES %s
               ON CONFLICT (content_id) DO UPDATE SET
                   content_hash = EXCLUDED.content_hash,
                   image_pending = EXCLUDED.image_pending,
                   image_absent = EXCLUDED.image_absent,
                   detail_common_pending = true, detail_intro_pending = true,
                   detail_info_pending = true,
                   embed_pending = true,
                   last_etl_at = now()""",
            [
                (
                    r.content_id,
                    r.content_hash,
                    r.first_image is not None,
                    r.first_image is None,
                )
                for r in rows
            ],
        )

    # ── pending 큐 ─────────────────────────────────────────

    def fetch_pending(self, kind: str, limit: int) -> list[int]:
        column = _PENDING_COLUMNS[kind]
        with self._conn.cursor() as cur:
            cur.execute(
                f"SELECT content_id FROM etl_spot_state WHERE {column} ORDER BY content_id LIMIT %s",
                (limit,),
            )
            return [row[0] for row in cur.fetchall()]

    def clear_pending(self, kind: str, content_ids: list[int]) -> None:
        if not content_ids:
            return
        column = _PENDING_COLUMNS[kind]
        with self._conn.cursor() as cur:
            cur.execute(
                f"UPDATE etl_spot_state SET {column} = false, last_etl_at = now()"
                " WHERE content_id = ANY(%s)",
                (content_ids,),
            )

    def fetch_missing_image_content_ids(self, limit: int) -> list[int]:
        """이미지 행이 없고 원천 부재로 확인되지 않은 콘텐츠만 복구 대상으로 반환한다."""
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT spot.content_id
                   FROM tourist_spot spot
                   INNER JOIN etl_spot_state state ON state.content_id = spot.content_id
                   WHERE NOT state.image_absent
                     AND NOT EXISTS (
                         SELECT 1 FROM spot_image image
                         WHERE image.content_id = spot.content_id
                     )
                   ORDER BY spot.content_id
                   LIMIT %s""",
                (limit,),
            )
            return [row[0] for row in cur.fetchall()]

    def mark_image_absent(self, content_id: int, absent: bool) -> None:
        """detailImage2 정상 응답의 이미지 존재 여부를 기록한다."""
        with self._conn.cursor() as cur:
            cur.execute(
                """UPDATE etl_spot_state
                   SET image_absent = %s, last_etl_at = now()
                   WHERE content_id = %s""",
                (absent, content_id),
            )

    # ── 상세 정보 반영 ──────────────────────────────────────

    def apply_detail_common(self, row: DetailCommonRow) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE tourist_spot SET homepage = %s, overview = %s WHERE content_id = %s",
                (row.homepage, row.overview, row.content_id),
            )

    def apply_detail_intro(self, row: DetailIntroRow) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """UPDATE tourist_spot
                   SET infocenter = %s, usetime = %s, restdate = %s,
                       parking = %s, expguide = %s
                   WHERE content_id = %s""",
                (row.infocenter, row.usetime, row.restdate, row.parking,
                 row.expguide, row.content_id),
            )

    def update_detail_info(self, content_id: int, items: list[dict]) -> None:
        """detailInfo2 반복 항목 전체를 tourist_spot JSONB 배열로 저장한다."""
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE tourist_spot SET detail_info = %s::jsonb WHERE content_id = %s",
                (json.dumps(items, ensure_ascii=False), content_id),
            )

    def replace_spot_images(self, content_id: int, rows: list[SpotImageRow]) -> int:
        """이미지 전체 교체 — 삭제 후 삽입으로 원천과 동일 상태를 보장한다."""
        with self._conn.cursor() as cur:
            cur.execute("DELETE FROM spot_image WHERE content_id = %s", (content_id,))
        return self._upsert(
            """INSERT INTO spot_image (content_id, "order", type, image_url) VALUES %s
               ON CONFLICT (content_id, "order") DO UPDATE
               SET type = EXCLUDED.type, image_url = EXCLUDED.image_url""",
            [(r.content_id, r.order, r.type, r.image_url) for r in rows],
        )

    def update_first_image_if_missing(self, content_id: int, image_url: str) -> None:
        """목록 API 대표 이미지가 없을 때만 상세 이미지 첫 장으로 보완한다."""
        with self._conn.cursor() as cur:
            cur.execute(
                """UPDATE tourist_spot
                   SET first_image = %s
                   WHERE content_id = %s AND first_image IS NULL""",
                (image_url, content_id),
            )

    # ── Spring AI 처리 통지 대상 ────────────────────────────

    def fetch_ready_for_ai(self, limit: int) -> list[int]:
        """상세 보강이 끝났고 AI 처리(embed_pending)가 남은 content_id.

        embed_pending 은 Spring 이 처리 완료 후 직접 false 로 내린다 — ETL 은 통지만 한다.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT content_id FROM etl_spot_state
                   WHERE embed_pending
                     AND NOT detail_common_pending
                     AND NOT detail_intro_pending
                     AND NOT detail_info_pending
                   ORDER BY content_id LIMIT %s""",
                (limit,),
            )
            return [row[0] for row in cur.fetchall()]

    # ── 실행 이력 ──────────────────────────────────────────

    def start_run(self, run_id: str, dag_id: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO etl_run (run_id, dag_id) VALUES (%s, %s)
                   ON CONFLICT (dag_id, run_id) DO UPDATE
                   SET status = 'running', started_at = now(), finished_at = NULL""",
                (run_id, dag_id),
            )

    def finish_run(self, run_id: str, dag_id: str, status: str, stats: dict) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """UPDATE etl_run
                   SET status = %s, finished_at = now(), stats = %s::jsonb
                   WHERE dag_id = %s AND run_id = %s""",
                (status, json.dumps(stats, ensure_ascii=False), dag_id, run_id),
            )

    # ── 공통 ───────────────────────────────────────────────

    def _upsert(self, sql: str, values: list[tuple], template: str | None = None) -> int:
        if not values:
            return 0
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, values, template=template, page_size=500)
        return len(values)
