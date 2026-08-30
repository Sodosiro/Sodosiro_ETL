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
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="course_status_sync",
    description="KST 자정에 전날 종료된 여행을 FINISHED 상태로 전환",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2026, 8, 21, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["course", "status", "batch"],
) as dag:
    finish_expired_courses = PythonOperator(
        task_id="finish_expired_courses",
        python_callable=tasks.finish_expired_courses,
        doc_md="KST 기준 end_date가 전날 이하인 미종료 코스를 FINISHED로 전환합니다.",
    )
