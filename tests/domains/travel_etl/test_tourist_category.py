import unittest

from src.domains.travel_etl.constants.tourist_category import (
    TouristCategory,
    classify_tourist_category,
)


class TouristCategoryTest(unittest.TestCase):
    def test_classifies_all_source_top_level_categories(self) -> None:
        cases = {
            ("FD", "FD01"): TouristCategory.RESTAURANT,
            ("FD", "FD05"): TouristCategory.CAFE,
            ("SH", "SH01"): TouristCategory.SHOPPING,
            ("VE", "VE01"): TouristCategory.TOURIST_ATTRACTION,
            ("HS", "HS01"): TouristCategory.TOURIST_ATTRACTION,
            ("C01", "C0112"): TouristCategory.TOURIST_ATTRACTION,
            ("EV", "EV01"): TouristCategory.TOURIST_ATTRACTION,
            ("NA", "NA01"): TouristCategory.NATURE,
            ("EX", "EX01"): TouristCategory.ACTIVITY,
            ("LS", "LS01"): TouristCategory.ACTIVITY,
            ("AC", "AC01"): TouristCategory.ACCOMMODATION,
        }
        for source_codes, expected in cases.items():
            with self.subTest(source_codes=source_codes):
                self.assertEqual(classify_tourist_category(*source_codes), expected)

    def test_rejects_unknown_source_category(self) -> None:
        self.assertIsNone(classify_tourist_category("UNKNOWN", None))


if __name__ == "__main__":
    unittest.main()
