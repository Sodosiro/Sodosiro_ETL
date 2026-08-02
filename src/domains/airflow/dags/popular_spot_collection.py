"""인기 관광지 트렌드 수집 DAG.

매 정각 KST 에 강원도 18개 시·군의 블로그·카페 트렌드를 수집해
tourist_spot 과 매칭된 장소의 mention_score 를 누적하고
카테고리별 인기도 순위(rank_tag)를 갱신한다.

흐름:
    start_run
    → collect_search_texts        (Kakao 블로그·카페 검색, 18도시 × 6쿼리 ≤ 108 req)
    → analyze_and_aggregate       (Komoran 형태소 분석 + 채널 다양성 점수)
    → match_and_score_tourist_spots (카카오 로컬 검증 ≤ 360 req, tourist_spot 매칭)
    → calculate_popularity_rank   (mention 감쇠 + 종합 점수 + 카테고리별 순위 태그)
    → finalize                    (실행 이력 기록, trigger_rule=all_done)
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
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="popular_spot_collection",
    description="강원도 18개 시·군 블로그·카페 트렌드 → tourist_spot 매칭 → 인기도 순위 갱신",
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 7, 27, tz=KST),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["trend", "kakao", "popularity"],
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
            "형태소 분석(Komoran NNP·NNG) + 불용어·지역명 제거 + 채널 다양성 보너스.\n"
            "출현 빈도 ≥ 3 필터 후 상위 20개 추출. 집계 스냅샷 저장."
        ),
    )

    match_and_score_tourist_spots = PythonOperator(
        task_id="match_and_score_tourist_spots",
        python_callable=tasks.match_and_score_tourist_spots,
        doc_md=(
            "카카오 로컬 키워드 검색으로 좌표·카테고리 검증 (AT4·FD6·CE7·AD5).\n"
            "검증 성공 → tourist_spot 이름·좌표 매칭 → spot_popularity mention_score 누적."
        ),
    )

    calculate_popularity_rank = PythonOperator(
        task_id="calculate_popularity_rank",
        python_callable=tasks.calculate_popularity_rank,
        doc_md=(
            "mention_score 에 감쇠 계수 적용 → like/review/rating 가중 합산 →\n"
            "카테고리(1~7)별 RANK() → 상위 10위에 '인기 {카테고리} {N}위' 태그 부여."
        ),
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=tasks.finalize,
        trigger_rule="all_done",
    )

    (
        start_run
        >> collect_search_texts
        >> analyze_and_aggregate
        >> match_and_score_tourist_spots
        >> calculate_popularity_rank
        >> finalize
    )
