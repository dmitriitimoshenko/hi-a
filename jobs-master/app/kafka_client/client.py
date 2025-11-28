import json
import logging
from typing import Any, Callable

from confluent_kafka import Message, Producer

from .config import KafkaConfig, get_kafka_config


def _json_serializer(value: Any) -> tuple[bytes | None, str | None]:
    if value is None:
        return None, None

    data = json.dumps(value, ensure_ascii=False).encode("utf-8")

    return data, "application/json"


class KafkaClient:
    def __init__(
        self,
        config: KafkaConfig,
        *,
        key_serializer: Callable[[Any], tuple[bytes | None, str | None]] | None = None,
        value_serializer: Callable[[Any], tuple[bytes | None, str | None]] | None = None,
    ) -> None:
        self._config = config
        self._key_serializer = key_serializer or _json_serializer
        self._value_serializer = value_serializer or _json_serializer
        self._producer: Producer | None = None
        self._logger = logging.getLogger(__name__)

    def _ensure_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(self._config.producer_conf())

        return self._producer

    def publish(
        self,
        topic: str,
        *,
        key: Any = None,
        value: Any = None,
        headers: dict[str, str] | None = None,
        on_delivery: Callable[[Exception | None, Message], None] | None = None,
    ) -> None:
        producer = self._ensure_producer()
        key_data, _ = self._key_serializer(key)
        value_data, _ = self._value_serializer(value)
        kafka_headers: list[tuple[str, bytes | None]] | None = None

        if headers is not None:
            kafka_headers = [
                (name, val.encode("utf-8") if isinstance(val, str) else val)
                for name, val in headers.items()
            ]

        def _callback(err: Exception | None, msg: Message) -> None:
            if err is not None:
                self._logger.error(
                    "Failed to deliver message to %s: %s",
                    topic,
                    err,
                )
            elif self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug(
                    "Delivered message to %s [%s] @ %s",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )

            if on_delivery is not None:
                on_delivery(err, msg)

        producer.produce(
            topic=topic,
            key=key_data,
            value=value_data,
            headers=kafka_headers,
            callback=_callback,
        )
        producer.poll(0)

    def flush(self, timeout: float = 5.0) -> None:
        producer = self._producer
        if producer is None:
            return

        producer.flush(timeout)

    def close(self) -> None:
        self.flush()
        self._producer = None


def get_kafka_client() -> KafkaClient:
    config = get_kafka_config()
    client = KafkaClient(config)

    return client
