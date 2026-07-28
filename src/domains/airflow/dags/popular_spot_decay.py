"""인기 관광지 인기도 감쇠 DAG.

매일 자정 KST 에 kakao_spot 전체에 카테고리별 감쇠 계수를 적용하고
임계값(0.01) 미만 레코드를 자동 삭제한다.

감쇠 계수:
    AT4(관광명소) · AD5(숙박): × 0.85  → 약 30일 후 소멸
    FD6(음식점)  · CE7(카페):  × 0.70  → 약 13일 후 소멸

흐름:
    start_run
    → apply_decay_and_prune  (감쇠 적용 + 임계값 미만 삭제)
    → finalize               (실행 이력 기록)
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

import _popular_spot_tasks as tasks

KST = pendulum.timezone("Asia/Seoul")

default_args = {
    "owner": "sodosiro",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="popular_spot_decay",
    description="kakao_spot 인기도 점수 일일 감쇠 + 임계값 미만 자동 삭제",
    schedule="0 0 * * *",          # 매일 자정 KST
    start_date=pendulum.datetime(2026, 7, 27, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["trend", "decay"],
) as dag:

    start_run = PythonOperator(
        task_id="start_run",
        python_callable=tasks.start_run,
    )

    apply_decay_and_prune = PythonOperator(
        task_id="apply_decay_and_prune",
        python_callable=tasks.apply_decay_and_prune,
        doc_md=(
            "카테고리별 감쇠 계수를 popularity_score 에 곱한 뒤\n"
            "score < 0.01 인 레코드를 삭제한다 (자연 소멸)."
        ),
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=tasks.finalize,
        trigger_rule="all_done",
    )

    start_run >> apply_decay_and_prune >> finalize
