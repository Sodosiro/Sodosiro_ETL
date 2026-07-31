"""인기 관광지 트렌드 DB 접근 계층.

- trending_spot: 형태소 분석 중간 집계 (파이프라인 내부)
- kakao_spot: 카카오맵 검증을 통과한 최종 장소 (사용자 노출)
- etl_run: 실행 이력 (travel_repository 와 동일 테이블 공유)
"""
from __future__ import annotations

import json
import logging
import re
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Iterator

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)


@dataclass
class TrendingSpotRow:
    keyword: str
    city_name: str
    blog_frequency: int
    cafe_frequency: int
    multi_channel: bool
    promoted: bool = False


@dataclass
class KakaoSpotRow:
    kakao_place_id: str
    place_name: str
    city_name: str
    address_name: str
    category_group_code: str
    category_group_name: str
    longitude: float
    latitude: float
    blog_mention_count: int
    cafe_mention_count: int
    popularity_score: float
    phone: str = ""


@dataclass(frozen=True)
class TrendSourceDocumentRow:
    """한 번만 분석해야 하는 카카오 검색 원천 문서."""

    city_name: str
    channel: str
    source_url: str
    published_at: str | None
    first_run_id: str


@dataclass(frozen=True)
class KakaoSpotImageTarget:
    id: int
    kakao_place_id: str
    place_name: str
    city_name: str


@dataclass(frozen=True)
class KakaoSpotImageRow:
    kakao_spot_id: int
    image_url: str
    source_doc_url: str | None
    source_site_name: str
    source_type: str
    tourist_content_id: int | None = None


def _merge_kakao_spot_rows(rows: list[KakaoSpotRow]) -> list[KakaoSpotRow]:
    """같은 카카오 장소로 매칭된 키워드의 수치를 하나로 합친다.

    PostgreSQL은 하나의 ``INSERT .. ON CONFLICT DO UPDATE`` 문에서 같은
    conflict key를 두 번 갱신할 수 없다. 서로 다른 트렌드 키워드가 같은
    ``kakao_place_id``를 반환하는 것은 정상적인 경우이므로, 저장 전에
    언급 수와 점수를 합산해 장소당 한 행만 남긴다.
    """
    merged: dict[str, KakaoSpotRow] = {}
    for row in rows:
        existing = merged.get(row.kakao_place_id)
        if existing is None:
            merged[row.kakao_place_id] = row
            continue

        merged[row.kakao_place_id] = KakaoSpotRow(
            kakao_place_id=existing.kakao_place_id,
            place_name=existing.place_name,
            city_name=existing.city_name,
            address_name=existing.address_name,
            category_group_code=existing.category_group_code,
            category_group_name=existing.category_group_name,
            longitude=existing.longitude,
            latitude=existing.latitude,
            blog_mention_count=existing.blog_mention_count + row.blog_mention_count,
            cafe_mention_count=existing.cafe_mention_count + row.cafe_mention_count,
            popularity_score=existing.popularity_score + row.popularity_score,
            phone=existing.phone or row.phone,
        )
    return list(merged.values())


def _normalize_place_name(value: str) -> str:
    """공백·괄호 등 표기 차이를 제거해 장소명을 보수적으로 비교한다."""
    return re.sub(r"[^0-9a-z가-힣]", "", value.lower())


class TrendConnectionFactory:
    """DB 커넥션 생성 책임 분리."""

    def __init__(self, db_url: str) -> None:
        self._db_url = db_url

    @contextmanager
    def open(self) -> Iterator["TrendRepository"]:
        conn = psycopg2.connect(self._db_url)
        try:
            yield TrendRepository(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


class TrendRepository:
    """trending_spot + kakao_spot + etl_run 저장소."""

    def __init__(self, conn) -> None:
        self._conn = conn

    def commit(self) -> None:
        self._conn.commit()

    def rollback(self) -> None:
        self._conn.rollback()

    # ── 원천 문서 이력 ───────────────────────────────────────

    def find_seen_source_urls(
        self, city_name: str, channel: str, source_urls: list[str]
    ) -> set[str]:
        """이전 성공 수집에서 이미 확인한 URL 집합을 반환한다."""
        if not source_urls:
            return set()
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT source_url
                   FROM trend_source_document
                   WHERE city_name = %s
                     AND channel = %s
                     AND source_url = ANY(%s)""",
                (city_name, channel, source_urls),
            )
            return {row[0] for row in cur.fetchall()}

    def insert_source_documents(self, rows: list[TrendSourceDocumentRow]) -> int:
        """원천 문서 URL을 멱등적으로 기록한다."""
        if not rows:
            return 0
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO trend_source_document
                       (city_name, channel, source_url, published_at, first_run_id)
                   VALUES %s
                   ON CONFLICT (city_name, channel, source_url) DO NOTHING""",
                [
                    (r.city_name, r.channel, r.source_url, r.published_at, r.first_run_id)
                    for r in rows
                ],
                template="(%s, %s, %s, %s::timestamptz, %s)",
            )
            return cur.rowcount

    # ── trending_spot ────────────────────────────────────────

    def upsert_trending_spots(self, rows: list[TrendingSpotRow]) -> int:
        """배치 단위 upsert — keyword + city_name 복합 UK 기준."""
        if not rows:
            return 0
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO trending_spot
                       (keyword, city_name, blog_frequency, cafe_frequency,
                        multi_channel, promoted, collected_at)
                   VALUES %s
                   ON CONFLICT (keyword, city_name) DO UPDATE SET
                       blog_frequency  = EXCLUDED.blog_frequency,
                       cafe_frequency  = EXCLUDED.cafe_frequency,
                       multi_channel   = EXCLUDED.multi_channel,
                       promoted        = EXCLUDED.promoted,
                       collected_at    = now()""",
                [
                    (r.keyword, r.city_name, r.blog_frequency, r.cafe_frequency,
                     r.multi_channel, r.promoted)
                    for r in rows
                ],
                template="(%s, %s, %s, %s, %s, %s, now())",
            )
        return len(rows)

    def mark_promoted(self, keyword: str, city_name: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE trending_spot SET promoted = true WHERE keyword = %s AND city_name = %s",
                (keyword, city_name),
            )

    # ── kakao_spot ───────────────────────────────────────────

    def upsert_kakao_spots(self, rows: list[KakaoSpotRow]) -> list[tuple[int, bool]]:
        """Upsert — 기존 행은 mention_count·popularity_score 누적 가산.

        반환: [(id, is_new), ...] — is_new=True 이면 이번 배치에서 신규 삽입된 행.
        """
        if not rows:
            return []
        unique_rows = _merge_kakao_spot_rows(rows)
        if len(unique_rows) != len(rows):
            logger.info(
                "카카오 장소 배치 중복 병합: 입력 %d건 → 저장 %d건",
                len(rows),
                len(unique_rows),
            )
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO kakao_spot (
                       kakao_place_id, place_name, city_name, address_name,
                       category_group_code, category_group_name,
                       longitude, latitude, phone,
                       blog_mention_count, cafe_mention_count, popularity_score,
                       collected_at
                   ) VALUES %s
                   ON CONFLICT (kakao_place_id) DO UPDATE SET
                       place_name          = EXCLUDED.place_name,
                       phone               = COALESCE(NULLIF(EXCLUDED.phone, ''), kakao_spot.phone),
                       blog_mention_count  = kakao_spot.blog_mention_count + EXCLUDED.blog_mention_count,
                       cafe_mention_count  = kakao_spot.cafe_mention_count + EXCLUDED.cafe_mention_count,
                       popularity_score    = kakao_spot.popularity_score + EXCLUDED.popularity_score,
                       collected_at        = EXCLUDED.collected_at
                   RETURNING id, (xmax = 0) AS is_new""",
                [
                    (
                        r.kakao_place_id, r.place_name, r.city_name, r.address_name,
                        r.category_group_code, r.category_group_name,
                        r.longitude, r.latitude, r.phone,
                        r.blog_mention_count, r.cafe_mention_count, r.popularity_score,
                    )
                    for r in unique_rows
                ],
                template=(
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())"
                ),
                fetch=True,
            )
            return [(row[0], bool(row[1])) for row in cur.fetchall()]

    # ── kakao_spot_image ─────────────────────────────────────

    def find_kakao_spots_missing_image(self, limit: int) -> list[KakaoSpotImageTarget]:
        """이미지 링크가 없는 인기 장소를 최근 인기도 순으로 제한 조회한다."""
        with self._conn.cursor() as cur:
            cur.execute(
                """SELECT spot.id, spot.kakao_place_id, spot.place_name, spot.city_name
                   FROM kakao_spot spot
                   LEFT JOIN kakao_spot_image image ON image.kakao_spot_id = spot.id
                   WHERE image.id IS NULL
                   ORDER BY spot.popularity_score DESC, spot.id DESC
                   LIMIT %s""",
                (limit,),
            )
            return [KakaoSpotImageTarget(*row) for row in cur.fetchall()]

    def find_existing_tourist_image(self, place_name: str, city_name: str) -> tuple[int, str] | None:
        """동일 도시의 TourAPI 이미지를 표기 차이를 보정해 우선 반환한다.

        이름이 완전히 정규화 일치하는 경우를 먼저 고르고, 네 글자 이상인
        장소명만 포함 일치를 허용해 다른 짧은 이름의 장소 오매칭을 피한다.
        """
        city_keyword = city_name[:-1] if city_name.endswith(("시", "군")) else city_name
        normalized_name = _normalize_place_name(place_name)
        if not normalized_name:
            return None
        with self._conn.cursor() as cur:
            cur.execute(
                """WITH input AS (SELECT %s::text AS normalized_name)
                   SELECT spot.content_id,
                          COALESCE(NULLIF(spot.first_image, ''), (
                              SELECT image.image_url
                              FROM spot_image image
                              WHERE image.content_id = spot.content_id
                                AND NULLIF(image.image_url, '') IS NOT NULL
                              ORDER BY image."order" ASC
                              LIMIT 1
                          )) AS image_url
                   FROM tourist_spot spot
                   CROSS JOIN input
                   CROSS JOIN LATERAL (
                       SELECT regexp_replace(lower(spot.title), '[^0-9a-z가-힣]', '', 'g') AS normalized_title
                   ) title
                   WHERE (
                           title.normalized_title = input.normalized_name
                           OR (
                               char_length(input.normalized_name) >= 4
                               AND (
                                   position(input.normalized_name in title.normalized_title) > 0
                                   OR position(title.normalized_title in input.normalized_name) > 0
                               )
                           )
                         )
                     AND spot.addr1 ILIKE %s
                   ORDER BY
                       (title.normalized_title = input.normalized_name) DESC,
                       (NULLIF(spot.first_image, '') IS NOT NULL) DESC,
                       spot.content_id DESC
                   LIMIT 1""",
                (normalized_name, f"%{city_keyword}%"),
            )
            row = cur.fetchone()
        return (int(row[0]), row[1]) if row and row[1] else None

    def upsert_kakao_spot_image(self, row: KakaoSpotImageRow) -> None:
        """장소별 최종 선택 이미지 하나를 멱등적으로 저장한다."""
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO kakao_spot_image (
                       kakao_spot_id, tourist_content_id, image_url, source_doc_url,
                       source_site_name, source_type, collected_at
                   ) VALUES (%s, %s, %s, %s, %s, %s, now())
                   ON CONFLICT (kakao_spot_id) DO UPDATE SET
                       tourist_content_id = EXCLUDED.tourist_content_id,
                       image_url = EXCLUDED.image_url,
                       source_doc_url = EXCLUDED.source_doc_url,
                       source_site_name = EXCLUDED.source_site_name,
                       collected_at = now()""",
                (
                    row.kakao_spot_id, row.tourist_content_id, row.image_url,
                    row.source_doc_url, row.source_site_name, row.source_type,
                ),
            )

    # ── 감쇠 잡 ─────────────────────────────────────────────

    def apply_decay(
        self,
        decay_attraction: float,
        decay_restaurant: float,
        decay_cafe: float,
    ) -> int:
        """카테고리별 감쇠 계수를 적용한다."""
        with self._conn.cursor() as cur:
            cur.execute(
                """UPDATE kakao_spot
                   SET popularity_score = popularity_score *
                       CASE category_group_code
                           WHEN 'AT4' THEN %(attraction)s
                           WHEN 'AD5' THEN %(attraction)s
                           WHEN 'FD6' THEN %(restaurant)s
                           WHEN 'CE7' THEN %(cafe)s
                           ELSE %(attraction)s
                       END""",
                {"attraction": decay_attraction, "restaurant": decay_restaurant, "cafe": decay_cafe},
            )
            return cur.rowcount

    def delete_expired(self, threshold: float) -> int:
        """popularity_score 임계값 미만 레코드를 삭제한다 (자연 소멸)."""
        with self._conn.cursor() as cur:
            cur.execute(
                "DELETE FROM kakao_spot WHERE popularity_score < %s",
                (threshold,),
            )
            count = cur.rowcount
        logger.info("kakao_spot 자연 소멸: %d건 삭제 (threshold=%.4f)", count, threshold)
        return count

    # ── 실행 이력 (etl_run 공유) ──────────────────────────────

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
