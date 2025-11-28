import json
import logging
from typing import Any, Callable

from confluent_kafka import Message, Producer

from .config import KafkaConfig

logging.basicConfig(level=logging.INFO)


class KafkaClient:
    def __init__(
        self,
        config: KafkaConfig,
        key_serializer: Callable[[Any], tuple[bytes | None, str | None]] | None = None,
        value_serializer: Callable[[Any], tuple[bytes | None, str | None]] | None = None,
    ) -> None:
        self._config = config
        self._producer: Producer | None = None
        self._key_serializer = (
            key_serializer if key_serializer is not None else self._json_serializer
        )
        self._value_serializer = (
            value_serializer if value_serializer is not None else self._json_serializer
        )
        self._logger = logging.getLogger(__name__)

    def _ensure_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(self._config.producer_conf_as_dict())

        return self._producer

    def publish(
        self,
        topic: str,
        key: Any = None,
        value: Any = None,
        headers: dict[str, str] | None = None,
        on_delivery: Callable[[Exception | None, Message], None] | None = None,
    ) -> None:
        producer = self._ensure_producer()

        key_bytes, _ = self._key_serializer(key)
        value_bytes, _ = self._value_serializer(value)

        hdrs: list[tuple[str, bytes | None]] | None = None
        if headers:
            hdrs = [
                (k, v.encode("utf-8") if isinstance(v, str) else v)
                for k, v in headers.items()
            ]

        def _cb(err: Exception | None, msg: Message) -> None:
            if err is not None:
                self._logger.error("Kafka delivery failed: %s", err)
            else:
                self._logger.debug(
                    "Kafka delivered to %s [%d] @ %d",
                    msg.topic(),
                    msg.partition(),
                    msg.offset(),
                )
            if on_delivery:
                on_delivery(err, msg)

        producer.produce(
            topic=topic,
            key=key_bytes,
            value=value_bytes,
            headers=hdrs,
            callback=_cb,
        )
        producer.poll(0)

    def flush(self, timeout: float = 10.0) -> None:
        producer = self._producer
        if producer is not None:
            producer.flush(timeout)

    @staticmethod
    def _json_serializer(obj: Any) -> tuple[bytes | None, str | None]:
        if obj is None:
            result = (None, None)

            return result

        serialized = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        result = (serialized, "application/json")

        return result
