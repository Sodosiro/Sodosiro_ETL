import unittest
from types import SimpleNamespace
from unittest.mock import patch

from src.domains.trend.service.morpheme_service import MorphemeService


class _FakeKiwi:
    def tokenize(self, _text: str):
        return [
            SimpleNamespace(form="오죽헌", tag="NNP"),
            SimpleNamespace(form="경기", tag="NNP"),
            SimpleNamespace(form="이천", tag="NNP"),
            SimpleNamespace(form="용인시", tag="NNP"),
            SimpleNamespace(form="담양", tag="NNP"),
            SimpleNamespace(form="가족", tag="NNG"),
        ]


class MorphemeServiceTest(unittest.TestCase):
    def test_extracts_only_proper_nouns(self) -> None:
        with patch("src.domains.trend.service.morpheme_service._get_kiwi", return_value=_FakeKiwi()):
            frequencies = MorphemeService().extract_frequencies(["테스트 문장"])

        self.assertEqual(frequencies, {"오죽헌": 1})


if __name__ == "__main__":
    unittest.main()
