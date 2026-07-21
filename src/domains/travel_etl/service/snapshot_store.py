"""areaBasedList2 원천 스냅샷의 CSV 영속화.

스냅샷은 태스크 사이의 대용량 전달 수단이 아니라, 재처리와 감사에 쓰는
불변 입력물이다. Airflow XCom에는 이 파일의 경로만 저장한다.
"""
from __future__ import annotations

import csv
from datetime import date
from pathlib import Path


class AreaBasedSnapshotStore:
    """CSV 스냅샷을 원자적으로 쓰고 다시 읽는다."""

    def __init__(self, directory: str) -> None:
        self._directory = Path(directory)

    def path_for(self, execution_date: date) -> Path:
        """논리 실행일마다 하나의 재사용 가능한 원천 스냅샷 경로를 반환한다."""
        return self._directory / f"{execution_date:%Y%m%d}__area_based.csv"

    def write(self, items: list[dict], execution_date: date) -> Path:
        self._directory.mkdir(parents=True, exist_ok=True)
        target = self.path_for(execution_date)
        if target.is_file():
            return target
        temporary = target.with_suffix(".csv.tmp")
        fieldnames = sorted({key for item in items for key in item}) or ["_empty"]

        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(items)
        temporary.replace(target)
        return target

    @staticmethod
    def read(path: str) -> list[dict]:
        with Path(path).open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames == ["_empty"]:
                return []
            return [dict(row) for row in reader]
