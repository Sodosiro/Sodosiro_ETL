"""인기 검색어 Redis → DB sync DAG.

BE 가 Redis Sorted Set(``travel:search:trending:{yyyyMMdd}``)에 누적하는 일별 검색어
버킷을 매일 최근 N일치 읽어 ``search_keyword_trend`` 스냅샷 테이블로 멱등 동기화한다.
서빙(조회)은 계속 Redis 가 담당하고, 이 테이블은 내구성·추이 분석·재적재 소스다.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import _search_trend_tasks as tasks

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "sodosiro",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=15),
}

with DAG(
    dag_id="search_keyword_trend_sync",
    description="인기 검색어 Redis 일별 버킷을 DB 스냅샷으로 동기화",
    schedule="10 4 * * *",
    start_date=pendulum.datetime(2026, 8, 18, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["travel", "search", "trending", "sync"],
) as dag:
    sync_search_trend = PythonOperator(
        task_id="sync_search_trend",
        python_callable=tasks.sync_search_trend,
        doc_md="최근 N일 Redis 검색어 버킷을 search_keyword_trend 에 UPSERT 합니다.",
    )
