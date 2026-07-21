"""여행 콘텐츠 누락 이미지 복구 DAG — 수동 트리거 전용.

spot_image가 비어 있고 detailImage2 원천 무이미지로 확인되지 않은 콘텐츠만
contentId로 다시 조회한다. 호출 예산 보호를 위해 실행당 처리량을 제한한다.
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
    "execution_timeout": timedelta(hours=1),
}

with DAG(
    dag_id="travel_image_recovery",
    description="이미지가 누락된 여행 콘텐츠만 contentId로 재조회",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["travel", "etl", "image", "recovery"],
) as dag:
    start_run = PythonOperator(task_id="start_run", python_callable=tasks.start_run)

    recover_missing_images = PythonOperator(
        task_id="recover_missing_images",
        python_callable=tasks.recover_missing_images,
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=tasks.finalize,
        trigger_rule="all_done",
    )

    start_run >> recover_missing_images >> finalize
