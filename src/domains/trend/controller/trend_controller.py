"""Airflow DAG 태스크의 얇은 진입점 (Facade).

Controller 는 입력을 service 로 위임한다. Airflow DAG 에는 업무 구현이 노출되지 않는다.
"""
from __future__ import annotations

from datetime import date

from src.domains.trend.config.settings import TrendSettings
from src.domains.trend.service.trend_aggregation_service import TrendAggregationService


class TrendController:
    """트렌드 수집 태스크의 얇은 진입점."""

    def __init__(self, settings: TrendSettings | None = None) -> None:
        self._service = TrendAggregationService(settings)

    def start_run(self, run_id: str, dag_id: str) -> dict:
        return self._service.start_run(run_id, dag_id)

    def collect_search_texts(self, execution_date: date, run_id: str) -> dict:
        return self._service.collect_search_texts(execution_date, run_id)

    def analyze_and_aggregate(self, raw_path: str, execution_date: date, run_id: str) -> dict:
        return self._service.analyze_and_aggregate(raw_path, execution_date, run_id)

    def validate_and_promote(self, aggregated_path: str) -> dict:
        return self._service.validate_and_promote(aggregated_path)

    def notify_spring_embedding(self, run_id: str, new_spot_ids: list[int]) -> dict:
        return self._service.notify_spring_embedding(run_id, new_spot_ids)

    def apply_decay_and_prune(self) -> dict:
        return self._service.apply_decay_and_prune()

    def collect_spot_images(self) -> dict:
        return self._service.collect_spot_images()

    def finalize_run(self, run_id: str, dag_id: str, status: str, stats: dict) -> dict:
        return self._service.finalize_run(run_id, dag_id, status, stats)
