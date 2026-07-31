"""티스토리 공개 게시글에서 대표 이미지 URL만 추출한다.

원문 HTML과 이미지 바이트는 저장하지 않는다. 호출 전 각 블로그의 robots.txt를
확인하고, 허용되지 않거나 응답이 불명확하면 보수적으로 건너뛴다.
"""
from __future__ import annotations

import time
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

import requests

_USER_AGENT = "SodosiroImageBot/1.0 (+https://sodosiro.example/contact)"


class _ImageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.og_image: str | None = None
        self.images: list[tuple[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key.lower(): value or "" for key, value in attrs}
        if tag == "meta" and values.get("property", "").lower() == "og:image":
            self.og_image = values.get("content") or self.og_image
        if tag == "img":
            url = values.get("data-src") or values.get("src")
            if url:
                self.images.append((url, values.get("alt", "")))


class TistoryImageClient:
    """robots.txt 허용 티스토리 문서에서 장소명과 가장 가까운 이미지 URL을 선택한다."""

    def __init__(self, min_interval_sec: float, timeout: float = 10.0, session: requests.Session | None = None) -> None:
        self._min_interval_sec = min_interval_sec
        self._timeout = timeout
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": _USER_AGENT})
        self._last_request_by_host: dict[str, float] = {}

    def extract_image(self, document_url: str, query: str) -> str | None:
        """허용된 공개 원문에서 이미지 URL 하나를 반환한다. 미허용·실패는 None."""
        parsed = urlparse(document_url)
        if parsed.scheme != "https" or not _is_tistory_host(parsed.hostname):
            return None
        if not self._is_allowed(document_url):
            return None
        self._wait_for_host(parsed.netloc)
        try:
            response = self._session.get(document_url, timeout=self._timeout)
            response.raise_for_status()
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError):
            return None
        if "text/html" not in response.headers.get("Content-Type", "").lower():
            return None

        parser = _ImageParser()
        parser.feed(response.text)
        return _select_image(document_url, parser.og_image, parser.images, query)

    def _is_allowed(self, document_url: str) -> bool:
        parsed = urlparse(document_url)
        robots_url = f"https://{parsed.netloc}/robots.txt"
        self._wait_for_host(parsed.netloc)
        try:
            response = self._session.get(robots_url, timeout=self._timeout)
            if response.status_code != 200:
                return False
        except (requests.ConnectionError, requests.Timeout):
            return False
        robots = RobotFileParser()
        robots.parse(response.text.splitlines())
        return robots.can_fetch(_USER_AGENT, document_url)

    def _wait_for_host(self, host: str) -> None:
        previous = self._last_request_by_host.get(host)
        if previous is not None:
            remaining = self._min_interval_sec - (time.monotonic() - previous)
            if remaining > 0:
                time.sleep(remaining)
        self._last_request_by_host[host] = time.monotonic()


def _is_tistory_host(hostname: str | None) -> bool:
    return bool(hostname and hostname.lower().endswith(".tistory.com"))


def _select_image(
    document_url: str, og_image: str | None, images: list[tuple[str, str]], query: str
) -> str | None:
    """본문 alt 텍스트에 장소 검색어가 있으면 우선하고, 없으면 OG 이미지를 사용한다."""
    tokens = [token.lower() for token in query.split() if token]
    candidates: list[tuple[int, int, str]] = []
    for index, (raw_url, alt) in enumerate(images):
        image_url = _absolute_http_url(document_url, raw_url)
        if image_url is None:
            continue
        score = sum(token in alt.lower() for token in tokens)
        candidates.append((score, -index, image_url))
    if candidates:
        best = max(candidates)
        if best[0] > 0:
            return best[2]
    return _absolute_http_url(document_url, og_image) if og_image else (
        candidates[0][2] if candidates else None
    )


def _absolute_http_url(base_url: str, raw_url: str | None) -> str | None:
    if not raw_url:
        return None
    url = urljoin(base_url, raw_url)
    return url if urlparse(url).scheme in {"http", "https"} else None
