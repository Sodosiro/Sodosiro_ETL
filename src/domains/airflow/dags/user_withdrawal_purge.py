"""유예기간이 지난 탈퇴 회원의 데이터를 완전 삭제하는 DAG.

탈퇴 접수 시점에 개인정보 익명화·소셜 연결 해제는 BE 가 즉시 처리하고, 남은 소유 데이터(리뷰·디깅·좋아요·
코스·알림 등)는 이 DAG 가 유예기간 경과 후 지운다. 삭제 로직 자체는 BE(UserPurgeService)에 있고
여기서는 하루 한 번 트리거만 한다 — S3 이미지 정리와 tourist_spot 집계 보정이 함께 필요하기 때문이다.

자세한 내용: sodosiro-BE/docs/user-withdrawal-deferred-deletion.md
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import _user_purge_tasks as tasks

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "sodosiro",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="user_withdrawal_purge",
    description="유예기간이 지난 탈퇴 회원의 잔여 데이터를 완전 삭제",
    schedule="30 4 * * *",
    start_date=pendulum.datetime(2026, 8, 28, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["user", "withdrawal", "batch"],
) as dag:
    purge_withdrawn_users = PythonOperator(
        task_id="purge_withdrawn_users",
        python_callable=tasks.purge_withdrawn_users,
        doc_md="withdrawn_at 이 유예기간을 넘긴 회원의 리뷰·디깅·좋아요·코스·알림과 회원 행을 삭제합니다.",
    )
