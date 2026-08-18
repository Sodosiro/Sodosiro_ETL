"""인기 검색어 sync DAG 공용 태스크 콜러블.

DAG 파일은 여기의 얇은 함수만 연결하고 실제 로직은 controller → service 로 위임한다.
"""
from __future__ import annotations

from src.domains.search_trend.controller.search_trend_controller import (
    SearchTrendController,
)


def sync_search_trend(**context) -> dict:
    """Redis 일별 검색어 버킷을 최근 N일치 읽어 search_keyword_trend 로 sync 한다."""
    end_date = context["logical_date"].in_timezone("Asia/Seoul").date()
    return SearchTrendController().sync_recent_buckets(end_date)
