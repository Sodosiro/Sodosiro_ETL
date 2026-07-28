"""인기 관광지 트렌드 수집 파이프라인 서비스.

세 단계로 분리해 각 Airflow 태스크가 하나씩 실행한다.
  1. collect_search_texts  — Kakao 검색 API 호출 → 원천 스냅샷 저장
  2. analyze_and_aggregate — 형태소 분석·집계 → trending_spot 저장
  3. validate_and_promote  — 카카오 로컬 검증 → kakao_spot upsert
"""
from __future__ import annotations

import json
import logging
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterator

import requests

from src.core.rate_limiter import MinIntervalLimiter
from src.domains.trend.client.kakao_client import GuardedKakaoClient, KakaoClient, KakaoPlace
from src.domains.trend.config.settings import TrendSettings, get_trend_settings
from src.domains.trend.constants.gangwon_cities import GANGWON_CITIES, GangwonCity
from src.domains.trend.repository.trend_repository import (
    KakaoSpotRow,
    TrendConnectionFactory,
    TrendingSpotRow,
)
from src.domains.trend.service.morpheme_service import MorphemeService

logger = logging.getLogger(__name__)

_SEARCH_QUERIES = ("여행", "관광", "맛집")


# ── 집계 중간 구조체 ─────────────────────────────────────────

@dataclass
class AggregatedKeyword:
    keyword: str
    city_name: str
    blog_frequency: int
    cafe_frequency: int
    multi_channel: bool
    raw_score: float
    normalized_score: float


@dataclass
class CollectionStats:
    api_calls: int = 0
    cities_processed: int = 0
    raw_documents: int = 0
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "api_calls": self.api_calls,
            "cities_processed": self.cities_processed,
            "raw_documents": self.raw_documents,
            **self.extra,
        }


@dataclass
class AggregationStats:
    total_keywords: int = 0
    trending_spots_saved: int = 0
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {
            "total_keywords": self.total_keywords,
            "trending_spots_saved": self.trending_spots_saved,
            **self.extra,
        }


@dataclass
class PromotionStats:
    validated: int = 0
    promoted: int = 0
    rejected: int = 0
    api_calls: int = 0
    new_spot_ids: list[int] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "validated": self.validated,
            "promoted": self.promoted,
            "rejected": self.rejected,
            "api_calls": self.api_calls,
            "new_spot_ids": self.new_spot_ids,
        }


# ── 스냅샷 저장소 ───────────────────────────────────────────

class TrendSnapshotStore:
    """원천 JSON 스냅샷을 날짜별 디렉터리에 저장·읽기."""

    def __init__(self, snapshot_dir: str) -> None:
        self._base = Path(snapshot_dir)

    def raw_path(self, execution_date: date, run_id: str) -> Path:
        return self._base / str(execution_date) / f"raw_{run_id[:8]}.json"

    def aggregated_path(self, execution_date: date, run_id: str) -> Path:
        return self._base / str(execution_date) / f"aggregated_{run_id[:8]}.json"

    def write(self, path: Path, data: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        logger.info("스냅샷 저장: %s (%d bytes)", path, path.stat().st_size)

    @staticmethod
    def read(path: str | Path) -> dict:
        return json.loads(Path(path).read_text(encoding="utf-8"))


# ── 서비스 ──────────────────────────────────────────────────

class TrendAggregationService:
    """수집 → 분석·집계 → 검증·승격 세 단계를 조율한다."""

    def __init__(self, settings: TrendSettings | None = None) -> None:
        self._settings = settings or get_trend_settings()
        self._connections = TrendConnectionFactory(self._settings.db_url)
        self._morpheme = MorphemeService()
        self._store = TrendSnapshotStore(self._settings.snapshot_dir)

    @contextmanager
    def _kakao_session(self, sleep_sec: float) -> Iterator[GuardedKakaoClient]:
        client = KakaoClient(
            api_key=self._settings.kakao_rest_api_key,
            timeout=self._settings.request_timeout_sec,
            max_retries=self._settings.max_retries,
        )
        limiter = MinIntervalLimiter(sleep_sec)
        yield GuardedKakaoClient(client, limiter)

    # ── 태스크 1: 원천 수집 ─────────────────────────────────

    def collect_search_texts(self, execution_date: date, run_id: str) -> dict:
        """18개 도시 × 3쿼리 × 블로그·카페 검색 → 원천 JSON 저장."""
        raw_path = self._store.raw_path(execution_date, run_id)
        if raw_path.is_file():
            logger.info("원천 스냅샷 재사용: %s", raw_path)
            return {"raw_path": str(raw_path), "reused": True}

        stats = CollectionStats()
        cities_data: dict[str, dict] = {}

        # 블로그·카페 검색도 카카오 API 호출이므로 로컬 검색과 같은 limiter를
        # 공유한다. 0.2초 간격이면 초당 최대 5회만 전송된다.
        with self._kakao_session(self._settings.keyword_sleep_sec) as client:
            for city in GANGWON_CITIES:
                blog_texts, cafe_texts = self._search_city(client, city, stats)
                cities_data[city.name] = {"blog": blog_texts, "cafe": cafe_texts}
                stats.cities_processed += 1
                logger.info(
                    "[%s] 블로그 %d건 / 카페 %d건",
                    city.name, len(blog_texts), len(cafe_texts),
                )
                time.sleep(self._settings.city_sleep_sec)

        stats.api_calls = sum(len(v["blog"]) + len(v["cafe"]) for v in cities_data.values())
        stats.raw_documents = stats.api_calls

        payload = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "cities": cities_data,
        }
        self._store.write(raw_path, payload)
        result = {"raw_path": str(raw_path), "reused": False, **stats.as_dict()}
        logger.info("원천 수집 완료: %s", result)
        return result

    def _search_city(
        self, client: GuardedKakaoClient, city: GangwonCity, stats: CollectionStats
    ) -> tuple[list[str], list[str]]:
        """한 도시의 블로그·카페 텍스트 수집."""
        blog_texts: list[str] = []
        cafe_texts: list[str] = []
        for query in _SEARCH_QUERIES:
            q = f"{city.short_name} {query}"
            for doc in client.search_blog(q):
                blog_texts.append(f"{doc.get('title', '')} {doc.get('contents', '')}")
            for doc in client.search_cafe(q):
                cafe_texts.append(f"{doc.get('title', '')} {doc.get('contents', '')}")
        return blog_texts, cafe_texts

    # ── 태스크 2: 형태소 분석·집계 ─────────────────────────

    def analyze_and_aggregate(self, raw_path: str, execution_date: date, run_id: str) -> dict:
        """원천 스냅샷 → 형태소 분석 → 채널 다양성 점수 → trending_spot 저장."""
        payload = self._store.read(raw_path)
        cities_raw: dict[str, dict] = payload["cities"]

        aggregated_by_city: dict[str, list[AggregatedKeyword]] = {}
        stats = AggregationStats()

        for city_name, texts in cities_raw.items():
            keywords = self._aggregate_city(city_name, texts)
            aggregated_by_city[city_name] = keywords
            stats.total_keywords += len(keywords)

        trending_rows = [
            TrendingSpotRow(
                keyword=kw.keyword,
                city_name=kw.city_name,
                blog_frequency=kw.blog_frequency,
                cafe_frequency=kw.cafe_frequency,
                multi_channel=kw.multi_channel,
                promoted=False,
            )
            for keywords in aggregated_by_city.values()
            for kw in keywords
        ]
        with self._connections.open() as repo:
            stats.trending_spots_saved = repo.upsert_trending_spots(trending_rows)

        aggregated_path = self._store.aggregated_path(execution_date, run_id)
        self._store.write(
            aggregated_path,
            {
                "cities": {
                    city: [asdict(kw) for kw in kws]
                    for city, kws in aggregated_by_city.items()
                }
            },
        )
        result = {"aggregated_path": str(aggregated_path), **stats.as_dict()}
        logger.info("분석·집계 완료: %s", result)
        return result

    def _aggregate_city(self, city_name: str, texts: dict) -> list[AggregatedKeyword]:
        """블로그·카페 채널 빈도 집계 + 채널 다양성 보너스 → 정규화된 상위 N개."""
        blog_freq = self._morpheme.extract_frequencies(texts.get("blog", []))
        cafe_freq = self._morpheme.extract_frequencies(texts.get("cafe", []))

        all_keywords = set(blog_freq) | set(cafe_freq)
        scored: list[AggregatedKeyword] = []

        for kw in all_keywords:
            blog = blog_freq.get(kw, 0)
            cafe = cafe_freq.get(kw, 0)
            total_freq = blog + cafe
            if total_freq < self._settings.min_frequency:
                continue
            multi = bool(blog > 0 and cafe > 0)
            raw = total_freq * self._settings.channel_diversity_bonus if multi else float(total_freq)
            scored.append(
                AggregatedKeyword(
                    keyword=kw,
                    city_name=city_name,
                    blog_frequency=blog,
                    cafe_frequency=cafe,
                    multi_channel=multi,
                    raw_score=raw,
                    normalized_score=0.0,  # 아래에서 정규화
                )
            )

        scored.sort(key=lambda x: x.raw_score, reverse=True)
        top = scored[: self._settings.top_n]

        if not top:
            logger.info("[%s] NNP 후보 없음 — 카카오 장소 검증 건너뜀", city_name)
            return []

        max_score = top[0].raw_score
        for kw in top:
            kw.normalized_score = kw.raw_score / max_score if max_score > 0 else 0.0
        logger.info(
            "[%s] NNP 후보 %d건 중 상위 %d건 선정: %s",
            city_name,
            len(scored),
            len(top),
            ", ".join(
                f"{kw.keyword}(blog={kw.blog_frequency}, cafe={kw.cafe_frequency}, score={kw.normalized_score:.2f})"
                for kw in top
            ),
        )
        return top

    # ── 태스크 3: 카카오 로컬 검증 → kakao_spot 승격 ────────

    def validate_and_promote(self, aggregated_path: str) -> dict:
        """집계 결과 → 카카오 로컬 검증 → kakao_spot upsert."""
        payload = self._store.read(aggregated_path)

        stats = PromotionStats()
        city_map = {c.name: c for c in GANGWON_CITIES}

        with self._kakao_session(self._settings.keyword_sleep_sec) as client:
            for city_name, keywords_data in payload["cities"].items():
                city = city_map.get(city_name)
                if city is None:
                    logger.warning("알 수 없는 도시: %s — 건너뜀", city_name)
                    continue
                self._promote_city_keywords(client, city, keywords_data, stats)

        logger.info("검증·승격 완료: %s", stats.as_dict())
        return stats.as_dict()

    def _promote_city_keywords(
        self,
        client: GuardedKakaoClient,
        city: GangwonCity,
        keywords_data: list[dict],
        stats: PromotionStats,
    ) -> None:
        kakao_rows: list[KakaoSpotRow] = []
        validated_keywords: list[tuple[str, KakaoPlace, float, int, int]] = []

        for kw_data in keywords_data:
            keyword = kw_data["keyword"]
            logger.info("[%s] 카카오 장소 검증 요청: keyword=%s", city.name, keyword)
            place = client.search_local(
                keyword=keyword,
                x=city.longitude,
                y=city.latitude,
                radius=self._settings.search_radius_meters,
            )
            stats.validated += 1
            if place is None:
                stats.rejected += 1
                logger.info("[%s] 카카오 장소 검증 탈락: keyword=%s", city.name, keyword)
                continue

            logger.info(
                "[%s] 카카오 장소 검증 통과: keyword=%s, place=%s, place_id=%s, category=%s",
                city.name,
                keyword,
                place.place_name,
                place.kakao_place_id,
                place.category_group_code,
            )

            blog_f = kw_data["blog_frequency"]
            cafe_f = kw_data["cafe_frequency"]
            validated_keywords.append((keyword, place, kw_data["normalized_score"], blog_f, cafe_f))

        stats.api_calls += len(keywords_data)

        if not validated_keywords:
            return

        for keyword, place, norm_score, blog_f, cafe_f in validated_keywords:
            kakao_rows.append(
                KakaoSpotRow(
                    kakao_place_id=place.kakao_place_id,
                    place_name=place.place_name,
                    city_name=city.name,
                    address_name=place.address_name,
                    category_group_code=place.category_group_code,
                    category_group_name=place.category_group_name,
                    longitude=place.longitude,
                    latitude=place.latitude,
                    blog_mention_count=blog_f,
                    cafe_mention_count=cafe_f,
                    popularity_score=norm_score,
                )
            )

        with self._connections.open() as repo:
            results = repo.upsert_kakao_spots(kakao_rows)
            for keyword, place, _norm, _bf, _cf in validated_keywords:
                repo.mark_promoted(keyword, city.name)
            stats.promoted += len(results)
            stats.new_spot_ids.extend(spot_id for spot_id, is_new in results if is_new)

    # ── 감쇠 잡 ────────────────────────────────────────────

    def apply_decay_and_prune(self) -> dict:
        """카테고리별 감쇠 계수 적용 → 임계값 미만 삭제."""
        with self._connections.open() as repo:
            decayed = repo.apply_decay(
                decay_attraction=self._settings.decay_attraction,
                decay_restaurant=self._settings.decay_restaurant,
                decay_cafe=self._settings.decay_cafe,
            )
            pruned = repo.delete_expired(self._settings.decay_threshold)

        result = {
            "decayed": decayed,
            "pruned": pruned,
            "decay_attraction": self._settings.decay_attraction,
            "decay_restaurant": self._settings.decay_restaurant,
            "decay_cafe": self._settings.decay_cafe,
            "threshold": self._settings.decay_threshold,
        }
        logger.info("감쇠 완료: %s", result)
        return result

    # ── Spring 통지 ─────────────────────────────────────────

    def notify_spring_embedding(self, run_id: str, new_spot_ids: list[int]) -> dict:
        """신규 kakao_spot ID 목록을 Spring 임베딩 파이프라인에 통지한다."""
        if not new_spot_ids:
            return {"notified": 0}

        url = f"{self._settings.spring_base_url}/internal/etl/trend/refresh"
        payload = {"runId": run_id, "kakaoSpotIds": new_spot_ids}
        session = requests.Session()
        headers = {"X-Internal-ETL-Token": self._settings.spring_internal_etl_token}

        last_error: Exception | None = None
        for attempt in range(self._settings.max_retries + 1):
            try:
                resp = session.post(url, json=payload, headers=headers,
                                    timeout=self._settings.spring_timeout_sec)
                if resp.status_code >= 500:
                    raise RuntimeError(f"HTTP {resp.status_code}")
                resp.raise_for_status()
                accepted = resp.json().get("accepted", len(new_spot_ids))
                logger.info("Spring 통지 완료: %d건 수락", accepted)
                return {"notified": len(new_spot_ids), "accepted": accepted}
            except Exception as e:
                last_error = e
                if attempt < self._settings.max_retries:
                    delay = float(2**attempt)
                    logger.warning("Spring 통지 재시도 %d회차 (%s) — %.0fs 대기", attempt + 1, e, delay)
                    time.sleep(delay)

        logger.error("Spring 통지 최종 실패: %s", last_error)
        return {"notified": 0, "error": str(last_error)}

    # ── etl_run ─────────────────────────────────────────────

    def start_run(self, run_id: str, dag_id: str) -> dict:
        with self._connections.open() as repo:
            repo.start_run(run_id, dag_id)
        return {"run_id": run_id}

    def finalize_run(self, run_id: str, dag_id: str, status: str, stats: dict) -> dict:
        with self._connections.open() as repo:
            repo.finish_run(run_id, dag_id, status, stats)
        return stats
