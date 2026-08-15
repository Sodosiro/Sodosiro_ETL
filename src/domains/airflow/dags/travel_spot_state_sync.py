"""관광지 ETL 상태 복구 DAG.

``tourist_spot``은 남아 있지만 ``etl_spot_state``가 삭제되었을 때 누락 상태 행을
기본 pending 상태로 되살린다. 기존 정상 상태는 변경하지 않는다.
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
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="travel_spot_state_sync",
    description="tourist_spot 기준으로 누락된 etl_spot_state를 복구",
    schedule="30 3 * * *",
    start_date=pendulum.datetime(2026, 8, 15, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["travel", "etl", "recovery", "state"],
) as dag:
    start_run = PythonOperator(task_id="start_run", python_callable=tasks.start_run)

    synchronize_spot_states = PythonOperator(
        task_id="synchronize_spot_states",
        python_callable=tasks.synchronize_spot_states,
        doc_md="누락 상태 행만 기본 pending 상태로 복구하며, 기존 상태는 덮어쓰지 않습니다.",
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=tasks.finalize,
        trigger_rule="all_done",
    )

    start_run >> synchronize_spot_states >> finalize
