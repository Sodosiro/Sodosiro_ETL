"""Spring AI 임베딩 통지 DAG.

상세 정보 적재가 끝난 여행지 중 ``embed_pending`` 상태인 콘텐츠를
Spring 서버에 전달한다. Spring이 임베딩을 완료하고 pending 상태를 해제하며,
이 DAG는 한 실행당 기본 10건만 통지한다.
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
    dag_id="travel_embedding",
    description="상세 적재가 완료된 여행지를 Spring AI 임베딩 서버에 통지합니다",
    schedule=None,
    start_date=pendulum.datetime(2026, 7, 1, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["travel", "etl", "embedding"],
) as dag:
    start_run = PythonOperator(task_id="start_run", python_callable=tasks.start_run)

    notify_spring = PythonOperator(
        task_id="notify_spring",
        python_callable=tasks.notify_spring,
        doc_md="상세 적재가 완료된 embed_pending 콘텐츠를 Spring AI에 통지합니다. 기본 한도는 10건입니다.",
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=tasks.finalize,
        trigger_rule="all_done",
    )

    start_run >> notify_spring >> finalize
