from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import _review_request_tasks as tasks

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "sodosiro",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="review_request_notification",
    description="여행 종료 사용자에게 코스 단위 리뷰 작성 유도 알림 발송",
    schedule="10 19 * * *",
    start_date=pendulum.datetime(2026, 8, 21, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["notification", "review", "batch"],
) as dag:
    send_review_requests = PythonOperator(
        task_id="send_review_requests",
        python_callable=tasks.send_review_requests,
    )
