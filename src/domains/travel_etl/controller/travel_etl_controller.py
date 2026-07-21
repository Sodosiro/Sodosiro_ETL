"""Airflow가 호출하는 여행 ETL 진입점.

Controller는 입력을 service로 위임한다. Airflow DAG에는 업무 구현 세부사항이 노출되지 않는다.
"""
from __future__ import annotations

from datetime import date

from src.domains.travel_etl.config.settings import TravelEtlSettings
from src.domains.travel_etl.service.travel_etl_service import TravelEtlService


class TravelEtlController:
    """여행 ETL 태스크의 얇은 진입점(Facade)."""

    def __init__(self, settings: TravelEtlSettings | None = None) -> None:
        self._service = TravelEtlService(settings)

    def start_run(self, run_id: str, dag_id: str) -> dict:
        return self._service.start_run(run_id, dag_id)

    def sync_codes(self) -> dict:
        return self._service.sync_codes()

    def ensure_base_codes(self) -> dict:
        return self._service.ensure_base_codes()

    def download_spot_snapshot(self, execution_date: date) -> dict:
        return self._service.download_spot_snapshot(execution_date)

    def load_spot_snapshot(self, snapshot_path: str) -> dict:
        return self._service.load_spot_snapshot(snapshot_path)

    def enrich_detail_common(self) -> dict:
        return self._service.enrich_detail_common()

    def enrich_detail_intro(self) -> dict:
        return self._service.enrich_detail_intro()

    def collect_detail_info(self) -> dict:
        return self._service.collect_detail_info()

    def collect_images(self) -> dict:
        return self._service.collect_images()

    def recover_missing_images(self) -> dict:
        return self._service.recover_missing_images()

    def notify_spring(self, run_id: str) -> dict:
        return self._service.notify_spring(run_id)

    def finalize_run(self, run_id: str, dag_id: str, status: str, stats: dict) -> dict:
        return self._service.finalize_run(run_id, dag_id, status, stats)
