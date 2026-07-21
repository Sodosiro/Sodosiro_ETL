"""DAG 구조 검증 — 상세 태스크 직렬화(H-3), execution_timeout(H-4), 부트스트랩 태스크(H-5).

Airflow 는 Windows 로컬을 공식 지원하지 않으므로 import 이 불가능한 환경에서는 skip 한다.
"""
from __future__ import annotations

import importlib
import sys
import unittest
from datetime import timedelta
from pathlib import Path

DAGS_DIR = Path(__file__).resolve().parents[1] / "src" / "domains" / "airflow" / "dags"


def _import_dag(module_name: str):
    if str(DAGS_DIR) not in sys.path:
        sys.path.insert(0, str(DAGS_DIR))
    return importlib.import_module(module_name)


try:
    # DAG 정의에 실제로 필요한 모듈까지 import 가능해야 한다.
    # (Windows 는 `import airflow` 는 되지만 models.dag 에서 os.register_at_fork 로 실패)
    import airflow.models.dag  # noqa: F401
    AIRFLOW_AVAILABLE = True
except Exception:  # pragma: no cover - Windows 등 미지원 환경
    AIRFLOW_AVAILABLE = False


@unittest.skipUnless(AIRFLOW_AVAILABLE, "airflow 를 import 할 수 없는 환경")
class DailyDagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.dag = _import_dag("travel_daily_incremental").dag

    def test_원천_API_태스크는_직렬이다(self) -> None:
        """병렬이면 태스크별 rate limiter 로 합산 호출 속도가 배수로 늘어난다."""
        expected_chain = [
            "start_run", "ensure_base_codes", "snapshot_area_based", "load_spot_snapshot",
            "enrich_detail_common", "enrich_detail_intro", "collect_detail_info",
            "collect_images", "notify_spring", "finalize",
        ]
        for upstream, downstream in zip(expected_chain, expected_chain[1:]):
            self.assertEqual(
                self.dag.get_task(downstream).upstream_task_ids,
                {upstream},
                f"{downstream} 는 {upstream} 하나만 upstream 이어야 한다",
            )

    def test_모든_태스크에_execution_timeout_이_있다(self) -> None:
        for task in self.dag.tasks:
            self.assertEqual(task.execution_timeout, timedelta(hours=1), task.task_id)

    def test_부트스트랩_태스크가_스냅샷보다_앞선다(self) -> None:
        self.assertIn("ensure_base_codes", self.dag.task_ids)
        self.assertIn(
            "ensure_base_codes", self.dag.get_task("snapshot_area_based").upstream_task_ids
        )


@unittest.skipUnless(AIRFLOW_AVAILABLE, "airflow 를 import 할 수 없는 환경")
class OtherDagsTest(unittest.TestCase):
    def test_이미지_복구_DAG_타임아웃(self) -> None:
        dag = _import_dag("travel_image_recovery").dag
        for task in dag.tasks:
            self.assertEqual(task.execution_timeout, timedelta(hours=1), task.task_id)

    def test_임베딩_DAG_타임아웃(self) -> None:
        dag = _import_dag("travel_embedding").dag
        for task in dag.tasks:
            self.assertEqual(task.execution_timeout, timedelta(hours=1), task.task_id)


if __name__ == "__main__":
    unittest.main()
