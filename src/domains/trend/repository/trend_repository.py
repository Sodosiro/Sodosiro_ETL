"""인기 관광지 트렌드 DB 접근 계층.

- trend_source_document : 카카오 검색 원천 URL 중복 수집 방지
- spot_popularity        : tourist_spot 기반 인기도 점수·순위
- etl_run               : 실행 이력 (travel_repository 와 동일 테이블 공유)
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


@dataclass(frozen=True)
class TrendSourceDocumentRow:
    """한 번만 분석해야 하는 카카오 검색 원천 문서."""

    city_name: str
    channel: str
    source_url: str
    published_at: str | None
    first_run_id: str


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
    """trend_source_document + spot_popularity + etl_run 저장소."""

    def __init__(self, conn) -> None:
        self._conn = conn

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

    # ── tourist_spot 매칭 ────────────────────────────────────

    def find_tourist_spot(
        self, place_name: str, city_name: str, longitude: float, latitude: float
    ) -> tuple[int, int] | None:
        """장소명·도시·좌표로 tourist_spot 을 매칭한다.

        반환: (content_id, category) 또는 None.
        - 이름 정규화 일치(완전 > 부분 4자 이상) + 도시 주소 필터 + 1km 이내 좌표 우선.
        - 좌표가 없는 tourist_spot 은 이름·도시 일치만으로도 후보가 된다.
        """
        city_keyword = city_name[:-1] if city_name.endswith(("시", "군")) else city_name
        normalized_name = _normalize_place_name(place_name)
        if not normalized_name:
            return None
        with self._conn.cursor() as cur:
            cur.execute(
                """WITH input AS (SELECT %s::text AS normalized_name)
                   SELECT ts.content_id, ts.category
                   FROM tourist_spot ts
                   CROSS JOIN input
                   CROSS JOIN LATERAL (
                       SELECT regexp_replace(lower(ts.title), '[^0-9a-z가-힣]', '', 'g') AS norm_title
                   ) t
                   WHERE (
                       t.norm_title = input.normalized_name
                       OR (
                           char_length(input.normalized_name) >= 4
                           AND (
                               position(input.normalized_name IN t.norm_title) > 0
                               OR position(t.norm_title IN input.normalized_name) > 0
                           )
                       )
                   )
                   AND ts.addr1 ILIKE %s
                   AND (
                       ts.map_x IS NULL
                       OR ts.map_y IS NULL
                       OR 6371000 * acos(
                           LEAST(1.0,
                               cos(radians(%s)) * cos(radians(ts.map_y::float)) *
                               cos(radians(ts.map_x::float) - radians(%s)) +
                               sin(radians(%s)) * sin(radians(ts.map_y::float))
                           )
                       ) <= 1000
                   )
                   ORDER BY
                       (t.norm_title = input.normalized_name) DESC,
                       (ts.map_x IS NOT NULL AND ts.map_y IS NOT NULL) DESC
                   LIMIT 1""",
                (normalized_name, f"%{city_keyword}%", latitude, longitude, latitude),
            )
            row = cur.fetchone()
        return (int(row[0]), int(row[1])) if row else None

    # ── spot_popularity ──────────────────────────────────────

    def accumulate_mention_score(self, content_id: int, score: float) -> None:
        """매칭된 tourist_spot 의 mention_score 를 누적 가산한다 (멱등 upsert)."""
        with self._conn.cursor() as cur:
            cur.execute(
                """INSERT INTO spot_popularity (content_id, mention_score)
                   VALUES (%s, %s)
                   ON CONFLICT (content_id) DO UPDATE SET
                       mention_score = spot_popularity.mention_score + EXCLUDED.mention_score""",
                (content_id, score),
            )

    def decay_and_rank_popularity(
        self,
        mention_decay: float,
        mention_weight: float,
        like_weight: float,
        review_weight: float,
        rating_weight: float,
        top_rank_n: int,
    ) -> int:
        """mention_score 감쇠 → 종합 점수 계산 → 카테고리별 순위 태그 갱신.

        반환: 갱신된 spot_popularity 행 수.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                "UPDATE spot_popularity SET mention_score = mention_score * %s",
                (mention_decay,),
            )
            cur.execute(
                """WITH scored AS (
                       SELECT
                           sp.content_id,
                           ts.category,
                           (sp.mention_score * %(mw)s
                            + ts.like_count  * %(lw)s
                            + ts.review_count * %(rw)s
                            + ts.avg_rating::float * ts.review_count * %(rrw)s
                           ) AS pop_score
                       FROM spot_popularity sp
                       JOIN tourist_spot ts ON ts.content_id = sp.content_id
                   ),
                   ranked AS (
                       SELECT
                           content_id,
                           category,
                           pop_score,
                           ROW_NUMBER() OVER (
                               PARTITION BY category
                               ORDER BY pop_score DESC, content_id DESC
                           ) AS cat_rank
                       FROM scored
                   ),
                   cat_names (code, name) AS (
                       VALUES (1,'식당'), (2,'카페'), (3,'쇼핑'), (4,'관광지'),
                              (5,'자연'), (6,'액티비티'), (7,'숙박')
                   )
                   UPDATE spot_popularity sp
                   SET
                       popularity_score = r.pop_score,
                       category_rank    = r.cat_rank,
                       rank_tag         = CASE
                           WHEN r.cat_rank <= %(top_n)s
                           THEN '인기 ' || cn.name || ' ' || r.cat_rank || '위'
                           ELSE NULL
                       END,
                       calculated_at    = now()
                   FROM ranked r
                   JOIN cat_names cn ON cn.code = r.category
                   WHERE sp.content_id = r.content_id""",
                {
                    "mw": mention_weight,
                    "lw": like_weight,
                    "rw": review_weight,
                    "rrw": rating_weight,
                    "top_n": top_rank_n,
                },
            )
            updated = cur.rowcount
        logger.info("인기도 순위 갱신: %d건", updated)
        return updated

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
