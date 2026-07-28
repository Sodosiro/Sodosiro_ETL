"""인기 관광지 트렌드 DB 접근 계층.

- trending_spot: 형태소 분석 중간 집계 (파이프라인 내부)
- kakao_spot: 카카오맵 검증을 통과한 최종 장소 (사용자 노출)
- etl_run: 실행 이력 (travel_repository 와 동일 테이블 공유)
"""
from __future__ import annotations

import json
import logging
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
        with self._conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """INSERT INTO kakao_spot (
                       kakao_place_id, place_name, city_name, address_name,
                       category_group_code, category_group_name,
                       longitude, latitude,
                       blog_mention_count, cafe_mention_count, popularity_score,
                       collected_at
                   ) VALUES %s
                   ON CONFLICT (kakao_place_id) DO UPDATE SET
                       place_name          = EXCLUDED.place_name,
                       blog_mention_count  = kakao_spot.blog_mention_count + EXCLUDED.blog_mention_count,
                       cafe_mention_count  = kakao_spot.cafe_mention_count + EXCLUDED.cafe_mention_count,
                       popularity_score    = kakao_spot.popularity_score + EXCLUDED.popularity_score,
                       collected_at        = EXCLUDED.collected_at
                   RETURNING id, (xmax = 0) AS is_new""",
                [
                    (
                        r.kakao_place_id, r.place_name, r.city_name, r.address_name,
                        r.category_group_code, r.category_group_name,
                        r.longitude, r.latitude,
                        r.blog_mention_count, r.cafe_mention_count, r.popularity_score,
                    )
                    for r in rows
                ],
                template=(
                    "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())"
                ),
                fetch=True,
            )
            return [(row[0], bool(row[1])) for row in cur.fetchall()]

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
