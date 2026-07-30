"""인기 관광지 트렌드 수집 DAG.

매 정각 KST 에 강원도 18개 시·군의 블로그·카페 트렌드를 수집해
카카오맵 검증을 통과한 장소를 kakao_spot 테이블에 누적 집계한다.

흐름:
    start_run
    → collect_search_texts   (Kakao 블로그·카페 검색, 18도시 × 6쿼리 ≤ 108 req)
    → analyze_and_aggregate  (Komoran 형태소 분석 + 채널 다양성 점수)
    → validate_and_promote   (카카오 로컬 검증 ≤ 360 req, kakao_spot upsert)
    → notify_spring_embedding (신규 장소 Spring AI 통지)
    → finalize               (실행 이력 기록, trigger_rule=all_done)
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
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    # 카카오 API ≤ 468 req/h — 여유 있게 상한 설정
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="popular_spot_collection",
    description="강원도 18개 시·군 블로그·카페 트렌드 → 카카오맵 검증 → kakao_spot 누적 집계",
    schedule="0 * * * *",          # 매 정각 KST
    start_date=pendulum.datetime(2026, 7, 27, tz=KST),
    catchup=False,
    max_active_runs=1,             # 실행 중복 방지 (API 쿼터 보호)
    default_args=default_args,
    tags=["trend", "kakao", "collection"],
) as dag:

    start_run = PythonOperator(
        task_id="start_run",
        python_callable=tasks.start_run,
    )

    collect_search_texts = PythonOperator(
        task_id="collect_search_texts",
        python_callable=tasks.collect_search_texts,
        doc_md=(
            "Kakao 블로그·카페 검색 API 호출 (18도시 × 3쿼리 × 2채널 = 108 req 이하).\n"
            "도시 간 500ms 슬립. 결과 JSON 을 data/trend/raw/{date}/ 에 저장."
        ),
    )

    analyze_and_aggregate = PythonOperator(
        task_id="analyze_and_aggregate",
        python_callable=tasks.analyze_and_aggregate,
        doc_md=(
            "형태소 분석(kiwipiepy NNP·NNG) + 불용어·지역명 제거 + 채널 다양성 보너스.\n"
            "출현 빈도 ≥ 3 필터 후 상위 20개 추출. trending_spot 테이블에 저장."
        ),
    )

    validate_and_promote = PythonOperator(
        task_id="validate_and_promote",
        python_callable=tasks.validate_and_promote,
        doc_md=(
            "카카오 로컬 키워드 검색으로 좌표·카테고리 검증 (AT4·FD6·CE7·AD5).\n"
            "검증 성공 → kakao_spot UPSERT (popularity_score 누적 가산).\n"
            "키워드 간 200ms 슬립."
        ),
    )

    notify_spring_embedding = PythonOperator(
        task_id="notify_spring_embedding",
        python_callable=tasks.notify_spring_embedding,
        doc_md="신규 kakao_spot ID 를 Spring AI 임베딩 파이프라인에 통지합니다.",
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=tasks.finalize,
        trigger_rule="all_done",   # 중간 실패와 무관하게 실행 이력을 남긴다
    )

    (
        start_run
        >> collect_search_texts
        >> analyze_and_aggregate
        >> validate_and_promote
        # >> notify_spring_embedding
        >> finalize
    )
