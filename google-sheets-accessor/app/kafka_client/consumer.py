from __future__ import annotations

import logging
from typing import Iterable

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition

from .config import KafkaConfig

logger = logging.getLogger(__name__)


class KafkaConsumer:
    def __init__(
        self,
        config: KafkaConfig,
        group_id: str,
        *,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
    ) -> None:
        if not group_id:
            raise ValueError("group_id must be set for KafkaConsumer")

        conf: dict[str, str | bool] = {
            "bootstrap.servers": config.bootstrap_servers,
            "client.id": config.client_id,
            "group.id": group_id,
            "enable.auto.commit": enable_auto_commit,
            "auto.offset.reset": auto_offset_reset,
            "session.timeout.ms": 10000,
            "max.poll.interval.ms": 300000,
        }

        self._consumer = Consumer(conf)
        self._logger = logging.getLogger(__name__)

    def subscribe(self, topics: Iterable[str]) -> None:
        self._consumer.subscribe(list(topics))

    def poll(self, timeout: float = 1.0) -> Message | None:
        try:
            msg: Message | None = self._consumer.poll(timeout)
        except KafkaException as exc:
            self._logger.error("Kafka poll failed: %s", exc)
            return None

        if msg is None:
            return None

        if msg.error():
            self._logger.error("Kafka message error: %s", msg.error())
            return None

        return msg

    def commit(self, msg: Message) -> None:
        try:
            self._consumer.commit(message=msg, asynchronous=False)
        except KafkaException as exc:
            self._logger.warning("Failed to commit message: %s", exc)

    def list_topics(self, timeout: float = 10.0):
        return self._consumer.list_topics(timeout=timeout)

    def assignment(self) -> list[TopicPartition]:
        return self._consumer.assignment()

    def close(self) -> None:
        self._consumer.close()
