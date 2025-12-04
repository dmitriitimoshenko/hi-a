from __future__ import annotations

from datetime import timedelta

from app.kafka_client.client import KafkaClient
from app.redis_client.client import RedisClient

from .base import BaseButtonHandler, build_button_handler_dependencies


class ButtonAppliedHandler(BaseButtonHandler):
    def __init__(self, kafka_client: KafkaClient, redis_client: RedisClient) -> None:
        super().__init__(
            kafka_client=kafka_client,
            redis_client=redis_client,
            logger_name=__name__,
        )

    def _refresh_details_cache(self, *, redis_key: str, raw_details: str) -> None:
        ttl_seconds = int(timedelta(days=30).total_seconds())
        self._redis_client.set(redis_key, raw_details or "BROKEN_DATA", ttl_seconds)


def get_button_applied_handler() -> ButtonAppliedHandler:
    kafka_client, redis_client = build_button_handler_dependencies()
    handler = ButtonAppliedHandler(
        kafka_client=kafka_client,
        redis_client=redis_client,
    )

    return handler
