"""여행지 데이터 일일 증분 ETL DAG.

매일 00:05 KST 에 공공데이터포털의 강원도 전체 여행 콘텐츠를 수집해
목록 원천을 CSV로 보존한 뒤 변경분(content_hash 기준)만 업무 DB 에 UPSERT 하고
상세 API·이미지·임베딩을 보강한다.
신규 변경분이 없어도 이전 실행에서 남은 pending 작업은 계속 처리한다.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import _travel_tasks as tasks

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "sodosiro",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    # max_active_runs=1 이므로 태스크가 늘어지면 다음 실행까지 밀린다 — 상한을 명시한다.
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="travel_daily_incremental",
    description="여행지 공공데이터 일일 증분 수집·적재",
    schedule="5 0 * * *",
    start_date=pendulum.datetime(2026, 7, 1, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["travel", "etl"],
) as dag:
    start_run = PythonOperator(task_id="start_run", python_callable=tasks.start_run)

    ensure_base_codes = PythonOperator(
        task_id="ensure_base_codes",
        python_callable=tasks.ensure_base_codes,
        doc_md="시군구 코드가 비어 있으면(신규 환경) 지역·분류 코드표를 동기화합니다. 이미 있으면 API 호출 없이 통과합니다.",
    )

    snapshot_area_based = PythonOperator(
        task_id="snapshot_area_based", python_callable=tasks.download_spot_snapshot
    )

    load_spot_snapshot = PythonOperator(
        task_id="load_spot_snapshot", python_callable=tasks.load_spot_snapshot
    )

    enrich_detail_common = PythonOperator(
        task_id="enrich_detail_common", python_callable=tasks.enrich_detail_common
    )

    enrich_detail_intro = PythonOperator(
        task_id="enrich_detail_intro", python_callable=tasks.enrich_detail_intro
    )

    collect_detail_info = PythonOperator(
        task_id="collect_detail_info", python_callable=tasks.collect_detail_info
    )

    collect_images = PythonOperator(task_id="collect_images", python_callable=tasks.collect_images)

    notify_spring = PythonOperator(
        task_id="notify_spring", python_callable=tasks.notify_spring
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=tasks.finalize,
        trigger_rule="all_done",  # 중간 실패·skip 과 무관하게 실행 이력을 남긴다
    )

    # 상세 보강 태스크는 직렬로 실행한다 — 병렬이면 태스크별 MinIntervalLimiter 가
    # 각자 간격을 지켜 원천 API 합산 호출 속도가 배수로 늘고(쿼터·429 유발),
    # 같은 etl_spot_state 행을 두고 행 잠금 경합이 생긴다.
    (
        start_run
        >> ensure_base_codes
        >> snapshot_area_based
        >> load_spot_snapshot
        >> enrich_detail_common
        >> enrich_detail_intro
        >> collect_detail_info
        >> collect_images
        # >> notify_spring
        >> finalize
    )
