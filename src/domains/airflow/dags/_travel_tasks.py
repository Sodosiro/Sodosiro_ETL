"""여행지 ETL DAG 공용 태스크 콜러블.

DAG 파일은 여기의 얇은 함수만 연결하고 실제 로직은 controller를 거쳐
service 계층으로 위임한다.
"""
from __future__ import annotations

from src.domains.travel_etl.controller.travel_etl_controller import TravelEtlController

STAT_TASK_IDS = (
    "ensure_base_codes", "snapshot_area_based", "load_spot_snapshot",
    "enrich_detail_common", "enrich_detail_intro", "collect_detail_info", "collect_images",
    "recover_missing_images", "notify_spring",
)


def start_run(**context) -> dict:
    return TravelEtlController().start_run(context["run_id"], context["dag"].dag_id)


def sync_codes() -> dict:
    return TravelEtlController().sync_codes()


def ensure_base_codes() -> dict:
    """신규 환경 부트스트랩 — 시군구 코드가 없을 때만 코드표를 동기화한다."""
    return TravelEtlController().ensure_base_codes()


def download_spot_snapshot(**context) -> dict:
    """원천 목록을 CSV로 저장하고 XCom에는 작은 경로 정보만 남긴다."""
    execution_date = context["logical_date"].in_timezone("Asia/Seoul").date()
    return TravelEtlController().download_spot_snapshot(execution_date)


def load_spot_snapshot(**context) -> dict:
    snapshot = context["ti"].xcom_pull(task_ids="snapshot_area_based")
    if not snapshot or not snapshot.get("snapshot_path"):
        raise RuntimeError("areaBasedList2 스냅샷 경로를 찾을 수 없습니다")
    return TravelEtlController().load_spot_snapshot(snapshot["snapshot_path"])


def enrich_detail_common() -> dict:
    return TravelEtlController().enrich_detail_common()


def enrich_detail_intro() -> dict:
    return TravelEtlController().enrich_detail_intro()


def collect_detail_info() -> dict:
    return TravelEtlController().collect_detail_info()


def collect_images() -> dict:
    return TravelEtlController().collect_images()


def recover_missing_images() -> dict:
    return TravelEtlController().recover_missing_images()


def notify_spring(**context) -> dict:
    """적재 완료 후 변경분을 Spring(DATA_EXTRACT)에 통지 — AI 처리는 Spring 책임."""
    return TravelEtlController().notify_spring(context["run_id"])


def finalize(**context) -> dict:
    """실행 이력 기록 (trigger_rule=all_done — 중간 실패가 있어도 상태를 남긴다)."""
    ti = context["ti"]
    stats = {task_id: ti.xcom_pull(task_ids=task_id) for task_id in STAT_TASK_IDS}
    failed = [
        t.task_id
        for t in context["dag_run"].get_task_instances(state="failed")
        if t.task_id != ti.task_id
    ]
    status = "failed" if failed else "success"
    if failed:
        stats["failed_tasks"] = failed
    return TravelEtlController().finalize_run(context["run_id"], status, stats)
