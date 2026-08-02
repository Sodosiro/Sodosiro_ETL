"""인기 관광지 트렌드 수집 파이프라인 서비스.

  1. collect_search_texts      — Kakao 검색 API 호출 → 원천 스냅샷 저장
  2. analyze_and_aggregate     — 형태소 분석·집계 → 집계 스냅샷 저장
  3. match_and_score_tourist   — 카카오 로컬 검증 → tourist_spot 매칭 → mention_score 누적
  4. calculate_popularity_rank — mention_score 감쇠 → 종합 점수 → 카테고리별 순위 태그
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

from src.core.rate_limiter import MinIntervalLimiter
from src.domains.trend.client.kakao_client import GuardedKakaoClient, KakaoClient, KakaoPlace
from src.domains.trend.config.settings import TrendSettings, get_trend_settings
from src.domains.trend.constants.gangwon_cities import GANGWON_CITIES, GangwonCity
from src.domains.trend.repository.trend_repository import (
    TrendConnectionFactory,
    TrendRepository,
    TrendSourceDocumentRow,
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
    extra: dict = field(default_factory=dict)

    def as_dict(self) -> dict:
        return {"total_keywords": self.total_keywords, **self.extra}


@dataclass
class MatchStats:
    searched: int = 0
    unverified: int = 0   # 카카오 로컬 검증 탈락
    unmatched: int = 0    # tourist_spot 매칭 실패
    matched: int = 0
    api_calls: int = 0

    def as_dict(self) -> dict:
        return asdict(self)


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
    """수집 → 분석·집계 → 매칭·점수 누적 → 순위 계산 네 단계를 조율한다."""

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
            self._record_snapshot_source_documents(self._store.read(raw_path), run_id)
            logger.info("원천 스냅샷 재사용: %s", raw_path)
            return {"raw_path": str(raw_path), "reused": True}

        stats = CollectionStats()
        cities_data: dict[str, dict[str, list[dict]]] = {}

        with self._connections.open() as repo, self._kakao_session(
            self._settings.keyword_sleep_sec
        ) as client:
            for city in GANGWON_CITIES:
                blog_documents, cafe_documents = self._search_city(client, repo, city)
                cities_data[city.name] = {"blog": blog_documents, "cafe": cafe_documents}
                stats.cities_processed += 1
                logger.info(
                    "[%s] 신규 블로그 %d건 / 신규 카페 %d건",
                    city.name, len(blog_documents), len(cafe_documents),
                )
                time.sleep(self._settings.city_sleep_sec)

            stats.api_calls = client.calls_made
        stats.raw_documents = sum(
            len(docs)
            for city_docs in cities_data.values()
            for docs in city_docs.values()
        )

        payload = {
            "collected_at": datetime.now(timezone.utc).isoformat(),
            "cities": cities_data,
        }
        self._store.write(raw_path, payload)
        self._record_snapshot_source_documents(payload, run_id)
        result = {"raw_path": str(raw_path), "reused": False, **stats.as_dict()}
        logger.info("원천 수집 완료: %s", result)
        return result

    def _search_city(
        self, client: GuardedKakaoClient, repo: TrendRepository, city: GangwonCity
    ) -> tuple[list[dict], list[dict]]:
        documents_by_channel: dict[str, dict[str, dict]] = {"blog": {}, "cafe": {}}
        for query in _SEARCH_QUERIES:
            q = f"{city.short_name} {query}"
            for channel in ("blog", "cafe"):
                self._collect_new_search_documents(
                    client, repo, city, channel, q, documents_by_channel[channel]
                )
        return list(documents_by_channel["blog"].values()), list(documents_by_channel["cafe"].values())

    def _collect_new_search_documents(
        self,
        client: GuardedKakaoClient,
        repo: TrendRepository,
        city: GangwonCity,
        channel: str,
        query: str,
        documents_by_url: dict[str, dict],
    ) -> None:
        """최신순 페이지를 순회하다 이전 실행의 URL을 만나면 중단한다."""
        page_size = min(50, max(1, self._settings.search_page_size))
        max_pages = min(50, max(1, self._settings.search_max_pages))

        for page in range(1, max_pages + 1):
            documents = (
                client.search_blog(query, page=page, size=page_size)
                if channel == "blog"
                else client.search_cafe(query, page=page, size=page_size)
            )
            if not documents:
                return

            urls = [doc.get("url", "") for doc in documents if doc.get("url")]
            seen_before_run = repo.find_seen_source_urls(city.name, channel, urls)

            for doc in documents:
                source_url = doc.get("url", "")
                if not source_url:
                    logger.warning("[%s] URL 없는 %s 검색 문서 제외", city.name, channel)
                    continue
                if source_url in documents_by_url:
                    documents_by_url[source_url]["matched_queries"].append(query)
                    continue
                if source_url in seen_before_run:
                    logger.info(
                        "[%s] %s 최신순 수집 중 기존 URL 발견 — query=%s, page=%d에서 종료",
                        city.name, channel, query, page,
                    )
                    return

                documents_by_url[source_url] = {
                    "url": source_url,
                    "published_at": doc.get("datetime"),
                    "title": doc.get("title", ""),
                    "contents": doc.get("contents", ""),
                    "matched_queries": [query],
                }

            if len(documents) < page_size:
                return

    def _record_snapshot_source_documents(self, payload: dict, run_id: str) -> int:
        rows: list[TrendSourceDocumentRow] = []
        for city_name, channels in payload.get("cities", {}).items():
            for channel in ("blog", "cafe"):
                for document in channels.get(channel, []):
                    if not isinstance(document, dict) or not document.get("url"):
                        continue
                    rows.append(
                        TrendSourceDocumentRow(
                            city_name=city_name,
                            channel=channel,
                            source_url=document["url"],
                            published_at=document.get("published_at"),
                            first_run_id=run_id,
                        )
                    )
        with self._connections.open() as repo:
            saved = repo.insert_source_documents(rows)
        logger.info("원천 문서 이력 저장: %d건", saved)
        return saved

    # ── 태스크 2: 형태소 분석·집계 ─────────────────────────

    def analyze_and_aggregate(self, raw_path: str, execution_date: date, run_id: str) -> dict:
        """원천 스냅샷 → 형태소 분석 → 채널 다양성 점수 → 집계 스냅샷 저장."""
        payload = self._store.read(raw_path)
        cities_raw: dict[str, dict] = payload["cities"]

        aggregated_by_city: dict[str, list[AggregatedKeyword]] = {}
        stats = AggregationStats()

        for city_name, texts in cities_raw.items():
            keywords = self._aggregate_city(city_name, texts)
            aggregated_by_city[city_name] = keywords
            stats.total_keywords += len(keywords)

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
        blog_freq = self._morpheme.extract_frequencies(
            [self._document_text(doc) for doc in texts.get("blog", [])]
        )
        cafe_freq = self._morpheme.extract_frequencies(
            [self._document_text(doc) for doc in texts.get("cafe", [])]
        )

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
                    normalized_score=0.0,
                )
            )

        scored.sort(key=lambda x: x.raw_score, reverse=True)
        top = scored[: self._settings.top_n]

        if not top:
            logger.info("[%s] NNP 후보 없음 — 장소 매칭 건너뜀", city_name)
            return []

        max_score = top[0].raw_score
        for kw in top:
            kw.normalized_score = kw.raw_score / max_score if max_score > 0 else 0.0
        logger.info(
            "[%s] NNP 후보 %d건 중 상위 %d건 선정: %s",
            city_name, len(scored), len(top),
            ", ".join(
                f"{kw.keyword}(score={kw.normalized_score:.2f})"
                for kw in top
            ),
        )
        return top

    @staticmethod
    def _document_text(document: str | dict) -> str:
        if isinstance(document, str):
            return document
        return f"{document.get('title', '')} {document.get('contents', '')}"

    # ── 태스크 3: 카카오 로컬 검증 → tourist_spot 매칭 → mention_score 누적 ──

    def match_and_score_tourist_spots(self, aggregated_path: str) -> dict:
        """집계 결과 → 카카오 로컬 검증 → tourist_spot 매칭 → mention_score 누적."""
        payload = self._store.read(aggregated_path)
        stats = MatchStats()
        city_map = {c.name: c for c in GANGWON_CITIES}

        with self._kakao_session(self._settings.keyword_sleep_sec) as client:
            for city_name, keywords_data in payload["cities"].items():
                city = city_map.get(city_name)
                if city is None:
                    logger.warning("알 수 없는 도시: %s — 건너뜀", city_name)
                    continue
                self._match_city_keywords(client, city, keywords_data, stats)

        stats.api_calls = 0  # client 는 session 밖에서 접근 불가 — 추정값 유지
        logger.info("tourist_spot 매칭 완료: %s", stats.as_dict())
        return stats.as_dict()

    def _match_city_keywords(
        self,
        client: GuardedKakaoClient,
        city: GangwonCity,
        keywords_data: list[dict],
        stats: MatchStats,
    ) -> None:
        for kw_data in keywords_data:
            keyword = kw_data["keyword"]
            logger.info("[%s] 카카오 장소 검증 요청: keyword=%s", city.name, keyword)
            place: KakaoPlace | None = client.search_local(
                keyword=keyword,
                city_name=city.name,
                city_short_name=city.short_name,
                x=city.longitude,
                y=city.latitude,
                radius=self._settings.search_radius_meters,
            )
            stats.searched += 1

            if place is None:
                stats.unverified += 1
                logger.info("[%s] 카카오 검증 탈락: keyword=%s", city.name, keyword)
                continue

            with self._connections.open() as repo:
                matched = repo.find_tourist_spot(
                    place.place_name, city.name, place.longitude, place.latitude
                )

            if matched is None:
                stats.unmatched += 1
                logger.info(
                    "[%s] tourist_spot 매칭 실패: keyword=%s, place=%s",
                    city.name, keyword, place.place_name,
                )
                continue

            content_id, _ = matched
            norm_score = kw_data["normalized_score"]
            with self._connections.open() as repo:
                repo.accumulate_mention_score(content_id, norm_score)

            stats.matched += 1
            logger.info(
                "[%s] tourist_spot 매칭 성공: keyword=%s → content_id=%d, score=%.4f",
                city.name, keyword, content_id, norm_score,
            )

    # ── 태스크 4: 인기도 순위 계산 ──────────────────────────

    def calculate_popularity_rank(self) -> dict:
        """mention_score 감쇠 → 종합 점수 계산 → 카테고리별 순위 태그 부여."""
        with self._connections.open() as repo:
            updated = repo.decay_and_rank_popularity(
                mention_decay=self._settings.mention_decay_factor,
                mention_weight=self._settings.mention_weight,
                like_weight=self._settings.like_weight,
                review_weight=self._settings.review_weight,
                rating_weight=self._settings.rating_weight,
                top_rank_n=self._settings.popularity_top_rank_n,
            )
        result = {
            "updated": updated,
            "mention_decay": self._settings.mention_decay_factor,
            "top_rank_n": self._settings.popularity_top_rank_n,
        }
        logger.info("인기도 순위 계산 완료: %s", result)
        return result

    # ── etl_run ─────────────────────────────────────────────

    def start_run(self, run_id: str, dag_id: str) -> dict:
        with self._connections.open() as repo:
            repo.start_run(run_id, dag_id)
        return {"run_id": run_id}

    def finalize_run(self, run_id: str, dag_id: str, status: str, stats: dict) -> dict:
        with self._connections.open() as repo:
            repo.finish_run(run_id, dag_id, status, stats)
        return stats
