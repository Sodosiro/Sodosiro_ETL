"""코스 확정 시 Spring이 캐싱하는 사용자별 활성 코스 Redis 키를 무효화한다.

키 포맷은 sodosiro-BE의 ActiveCourseCache.redisKey(userId)와 반드시 동일해야 한다:
    user:{userId}:active-course

course_status_sync DAG가 코스를 FINISHED로 전환한 직후 호출해,
근처 찜 알림이 이미 끝난 여행 기준으로 계속 발송되지 않도록 한다.
"""
from __future__ import annotations

import redis


class ActiveCourseCacheRepository:
    """사용자별 활성 코스 캐시 키 삭제 전용 (BE가 쓰고, 여기서는 무효화만 한다)."""

    def __init__(self, redis_url: str) -> None:
        self._client = redis.Redis.from_url(redis_url, decode_responses=True)

    @staticmethod
    def _key(user_id: int) -> str:
        return f"user:{user_id}:active-course"

    def evict(self, user_ids: list[int]) -> int:
        """중복 user_id를 접어 DEL 한 번으로 지우고 삭제된 키 수를 반환한다."""
        if not user_ids:
            return 0
        keys = [self._key(user_id) for user_id in dict.fromkeys(user_ids)]
        return self._client.delete(*keys)

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:  # noqa: BLE001 - 정리 실패는 무시
            pass
