"""인기 관광지 트렌드 DAG 공용 태스크 콜러블.

DAG 파일은 여기의 얇은 함수만 연결하고 실제 로직은 controller → service 로 위임한다.
"""
from __future__ import annotations

from src.domains.trend.controller.trend_controller import TrendController

_COLLECTION_TASK_IDS = (
    "collect_search_texts",
    "analyze_and_aggregate",
    "match_and_score_tourist_spots",
    "calculate_popularity_rank",
)


# ── 공통 ─────────────────────────────────────────────────────

def start_run(**context) -> dict:
    return TrendController().start_run(context["run_id"], context["dag"].dag_id)


def finalize(**context) -> dict:
    ti = context["ti"]
    dag_id = context["dag"].dag_id
    stats = {task_id: ti.xcom_pull(task_ids=task_id) for task_id in _COLLECTION_TASK_IDS}
    failed = [
        t.task_id
        for t in context["dag_run"].get_task_instances(state="failed")
        if t.task_id != ti.task_id
    ]
    status = "failed" if failed else "success"
    if failed:
        stats["failed_tasks"] = failed
    return TrendController().finalize_run(
        context["run_id"], dag_id, status, stats
    )


# ── 수집 DAG 태스크 ───────────────────────────────────────────

def collect_search_texts(**context) -> dict:
    """18개 도시 × 블로그·카페 × 3쿼리 Kakao 검색 → 원천 JSON 저장."""
    execution_date = context["logical_date"].in_timezone("Asia/Seoul").date()
    run_id = context["run_id"]
    return TrendController().collect_search_texts(execution_date, run_id)


def analyze_and_aggregate(**context) -> dict:
    """형태소 분석 + 채널 다양성 점수 계산 → 집계 스냅샷 저장."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="collect_search_texts")
    if not result or not result.get("raw_path"):
        raise RuntimeError("원천 스냅샷 경로를 찾을 수 없습니다 (collect_search_texts XCom 누락)")
    execution_date = context["logical_date"].in_timezone("Asia/Seoul").date()
    return TrendController().analyze_and_aggregate(
        result["raw_path"], execution_date, context["run_id"]
    )


def match_and_score_tourist_spots(**context) -> dict:
    """카카오 로컬 검증 → tourist_spot 매칭 → spot_popularity mention_score 누적."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="analyze_and_aggregate")
    if not result or not result.get("aggregated_path"):
        raise RuntimeError("집계 스냅샷 경로를 찾을 수 없습니다 (analyze_and_aggregate XCom 누락)")
    return TrendController().match_and_score_tourist_spots(result["aggregated_path"])


def calculate_popularity_rank(**context) -> dict:
    """mention_score 감쇠 → 종합 인기도 점수 → 카테고리별 순위 태그 갱신."""
    return TrendController().calculate_popularity_rank()
