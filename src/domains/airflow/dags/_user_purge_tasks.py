from __future__ import annotations

import logging

from src.domains.travel_etl.client.spring_client import SpringClient
from src.domains.travel_etl.config.settings import get_settings

logger = logging.getLogger(__name__)


def purge_withdrawn_users(**context) -> dict:
    settings = get_settings()
    result = SpringClient(settings).post_withdrawn_user_purge(context["run_id"])
    logger.info("탈퇴 회원 데이터 완전 삭제 완료: %s", result)
    return result
