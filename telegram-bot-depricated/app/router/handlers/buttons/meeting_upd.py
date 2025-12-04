from __future__ import annotations

from app.kafka_client.client import KafkaClient
from app.redis_client.client import RedisClient

from .base import BaseButtonHandler, build_button_handler_dependencies


class ButtonMeetingUpdHandler(BaseButtonHandler):
    def __init__(self, redis_client: RedisClient, kafka_client: KafkaClient) -> None:
        super().__init__(
            kafka_client=kafka_client,
            redis_client=redis_client,
            logger_name=__name__,
        )


def get_button_meeting_upd_handler() -> ButtonMeetingUpdHandler:
    kafka_client, redis_client = build_button_handler_dependencies()
    handler = ButtonMeetingUpdHandler(
        redis_client=redis_client,
        kafka_client=kafka_client,
    )

    return handler
