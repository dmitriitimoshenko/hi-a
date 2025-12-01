from __future__ import annotations

import logging
from functools import lru_cache

from ..config import Config
from .config import get_kafka_config
from .producer import KafkaProducer


class EmbeddingPublisher:
    def __init__(
        self,
        producer: KafkaProducer,
        *,
        topic: str,
    ) -> None:
        if not topic:
            message = "Kafka topic for add-embedding-for-application is not configured"
            raise ValueError(message)

        self._producer = producer
        self._topic = topic
        self._logger = logging.getLogger(__name__)

    def enqueue(self, *, application_id: int, payload: str) -> None:
        try:
            self._producer.publish(
                self._topic,
                key=application_id,
                value=payload,
            )
        except Exception as e:
            self._logger.warning(
                "Failed to publish embedding job for application %d: %s",
                application_id,
                e,
            )


@lru_cache(maxsize=1)
def get_embedding_publisher() -> EmbeddingPublisher:
    kafka_config = get_kafka_config()
    producer = KafkaProducer(kafka_config)
    topic = Config.KAFKA_TOPIC_ADD_APPLICATION_EMBEDDING

    publisher = EmbeddingPublisher(
        producer,
        topic=topic,
    )

    return publisher
