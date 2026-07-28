"""인기 관광지 트렌드 DAG 공용 태스크 콜러블.

DAG 파일은 여기의 얇은 함수만 연결하고 실제 로직은 controller → service 로 위임한다.
"""
from __future__ import annotations

from src.domains.trend.controller.trend_controller import TrendController

_COLLECTION_TASK_IDS = (
    "collect_search_texts",
    "analyze_and_aggregate",
    "validate_and_promote",
    "notify_spring_embedding",
)

_DECAY_TASK_IDS = ("apply_decay_and_prune",)


# ── 공통 ─────────────────────────────────────────────────────

def start_run(**context) -> dict:
    return TrendController().start_run(context["run_id"], context["dag"].dag_id)


def finalize(**context) -> dict:
    ti = context["ti"]
    dag_id = context["dag"].dag_id
    task_ids = _COLLECTION_TASK_IDS if "collect" in dag_id else _DECAY_TASK_IDS
    stats = {task_id: ti.xcom_pull(task_ids=task_id) for task_id in task_ids}
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
    """형태소 분석 + 채널 다양성 점수 계산 + trending_spot 저장."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="collect_search_texts")
    if not result or not result.get("raw_path"):
        raise RuntimeError("원천 스냅샷 경로를 찾을 수 없습니다 (collect_search_texts XCom 누락)")
    execution_date = context["logical_date"].in_timezone("Asia/Seoul").date()
    return TrendController().analyze_and_aggregate(
        result["raw_path"], execution_date, context["run_id"]
    )


def validate_and_promote(**context) -> dict:
    """카카오 로컬 API 검증 → kakao_spot upsert."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="analyze_and_aggregate")
    if not result or not result.get("aggregated_path"):
        raise RuntimeError("집계 스냅샷 경로를 찾을 수 없습니다 (analyze_and_aggregate XCom 누락)")
    return TrendController().validate_and_promote(result["aggregated_path"])


def notify_spring_embedding(**context) -> dict:
    """신규 kakao_spot ID 목록을 Spring 임베딩 파이프라인에 통지."""
    ti = context["ti"]
    result = ti.xcom_pull(task_ids="validate_and_promote")
    new_spot_ids: list[int] = (result or {}).get("new_spot_ids", [])
    return TrendController().notify_spring_embedding(context["run_id"], new_spot_ids)


# ── 감쇠 DAG 태스크 ───────────────────────────────────────────

def apply_decay_and_prune(**context) -> dict:
    """카테고리별 감쇠 계수 적용 → popularity_score 임계값 미만 레코드 삭제."""
    return TrendController().apply_decay_and_prune()
