import unittest

from src.domains.travel_etl.repository.travel_repository import TravelRepository


class _Cursor:
    def __init__(self) -> None:
        self.executed: list[str] = []
        self.rowcount = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def execute(self, query: str) -> None:
        self.executed.append(query)

    def fetchone(self) -> tuple[int]:
        return (0,)


class _Connection:
    def __init__(self) -> None:
        self.cursor_instance = _Cursor()

    def cursor(self) -> _Cursor:
        return self.cursor_instance


class SynchronizeSpotStatesTest(unittest.TestCase):
    def test_sync_completes_only_states_with_a_current_valid_embedding(self) -> None:
        connection = _Connection()

        TravelRepository(connection).synchronize_spot_states()

        complete_query = connection.cursor_instance.executed[1]
        restore_query = connection.cursor_instance.executed[2]
        self.assertIn("SET embed_pending = false", complete_query)
        self.assertIn("embedding.created_at >= state.last_etl_at", complete_query)
        self.assertIn("embedding.embedding IS NOT NULL", complete_query)
        self.assertIn("embed_pending", restore_query)
        self.assertIn("NOT EXISTS", restore_query)
        self.assertIn("embedding.content_id = spot.content_id", restore_query)
        self.assertIn("embedding.embedding IS NOT NULL", restore_query)


if __name__ == "__main__":
    unittest.main()
