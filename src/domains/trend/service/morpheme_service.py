"""한국어 형태소 분석 서비스.

kiwipiepy(pip install kiwipiepy)를 사용한다.
NNP(고유명사)·NNG(일반명사)만 추출하고 불용어·지역명을 제거한 뒤 빈도를 반환한다.

Komoran(Java) 대응 Python 구현 — POS 태그 체계(NNP/NNG)가 동일하다.
"""
from __future__ import annotations

import logging
import re
from functools import lru_cache
from typing import TYPE_CHECKING

from src.domains.trend.constants.gangwon_cities import GANGWON_REGION_NAMES
from src.domains.trend.constants.travel_stopwords import TRAVEL_STOPWORDS

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

_TARGET_TAGS = {"NNP", "NNG"}
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_HTML_ENTITY_RE = re.compile(r"&[a-zA-Z]+;|&#\d+;")
_NON_KOREAN_RE = re.compile(r"[^가-힣 ]")


@lru_cache(maxsize=1)
def _get_kiwi():
    """Kiwi 인스턴스 — 초기화 비용이 크므로 싱글톤으로 관리한다."""
    try:
        from kiwipiepy import Kiwi  # type: ignore[import-untyped]
    except ImportError as e:
        raise ImportError(
            "kiwipiepy가 설치되지 않았습니다. 'pip install kiwipiepy' 를 실행하세요."
        ) from e
    return Kiwi()


def _preprocess(text: str) -> str:
    """HTML 태그·엔티티 제거 → 한글+공백만 추출."""
    text = _HTML_TAG_RE.sub(" ", text)
    text = _HTML_ENTITY_RE.sub(" ", text)
    return _NON_KOREAN_RE.sub(" ", text)


class MorphemeService:
    """텍스트 목록 → 명사 빈도 맵."""

    def extract_frequencies(self, texts: list[str]) -> dict[str, int]:
        """NNP·NNG 명사를 추출하고 빈도를 반환한다.

        반환: {명사: 빈도} — 불용어·지역명·2글자 미만 제거 후.
        """
        kiwi = _get_kiwi()
        freq: dict[str, int] = {}
        for text in texts:
            cleaned = _preprocess(text)
            if not cleaned.strip():
                continue
            try:
                tokens = kiwi.tokenize(cleaned)
            except Exception:
                logger.exception("형태소 분석 실패 — 해당 텍스트 건너뜀")
                continue
            for token in tokens:
                word = token.form
                if token.tag not in _TARGET_TAGS:
                    continue
                if len(word) < 2:
                    continue
                if word in TRAVEL_STOPWORDS:
                    continue
                if word in GANGWON_REGION_NAMES:
                    continue
                freq[word] = freq.get(word, 0) + 1
        return freq
