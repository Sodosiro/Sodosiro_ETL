from __future__ import annotations

import logging

from src.domains.travel_etl.client.spring_client import SpringClient
from src.domains.travel_etl.config.settings import get_settings

logger = logging.getLogger(__name__)


def send_review_requests(**context) -> dict:
    settings = get_settings()
    result = SpringClient(settings).post_review_requests(context["run_id"])
    logger.info("리뷰 작성 유도 알림 요청 완료: %s", result)
    return result
