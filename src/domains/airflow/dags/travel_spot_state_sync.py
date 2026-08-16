"""관광지 ETL 상태 복구 DAG.

``tourist_spot``은 남아 있지만 ``etl_spot_state``가 삭제되었을 때 누락 상태 행을
기본 pending 상태로 되살린다. 최신 유효 임베딩이 있는 pending 상태는 완료로 보정한다.
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
    description="누락된 ETL 상태를 복구하고 최신 임베딩 완료 상태를 보정",
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
        doc_md="누락 상태 행을 복구하고, 최신 유효 임베딩이 있는 pending 상태만 완료로 보정합니다.",
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=tasks.finalize,
        trigger_rule="all_done",
    )

    start_run >> synchronize_spot_states >> finalize
