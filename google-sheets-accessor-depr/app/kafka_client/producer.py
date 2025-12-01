from __future__ import annotations

import logging
from typing import Any

from confluent_kafka import Producer

from .config import KafkaConfig


class KafkaProducer:
    def __init__(self, config: KafkaConfig) -> None:
        self._config = config
        self._producer = Producer(self._config.producer_conf())
        self._logger = logging.getLogger(__name__)

    def publish(
        self,
        topic: str,
        *,
        key: Any = None,
        value: Any = None,
    ) -> None:
        key_bytes = (
            str(key).encode("utf-8")
            if key is not None
            else None
        )
        value_bytes = (
            str(value).encode("utf-8")
            if value is not None
            else None
        )

        def _callback(err: Exception | None, msg) -> None:
            if err is not None:
                self._logger.error("Kafka delivery failed: %s", err)
            else:
                self._logger.debug(
                    "Kafka delivered to %s [%d] @ %d",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )

        self._producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            callback=_callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 5.0) -> None:
        self._producer.flush(timeout)
