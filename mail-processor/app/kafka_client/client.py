import json
import logging

from typing import Any, Callable, Iterable
from confluent_kafka import (
    Producer,
    Consumer,
    Message,
    TopicPartition,
    OFFSET_BEGINNING,
)

from .config import KafkaConfig, get_kafka_config

logging.basicConfig(level=logging.INFO)


def _default_json_deserializer(payload: bytes | None) -> Any:
    if payload is None:
        result = None

        return result
    try:
        result = json.loads(payload.decode("utf-8"))

        return result
    except Exception:
        result = payload

        return result


class KafkaClient:
    def __init__(
        self,
        config: KafkaConfig,
        group_id: str | None = None,
        *,
        auto_offset_reset: str = "earliest",
        enable_auto_commit: bool = True,
        key_serializer: Callable[[Any], tuple[bytes | None, str | None]] | None = None,
        value_serializer: Callable[[Any], tuple[bytes | None, str | None]] | None = None,
        key_deserializer: Callable[[bytes | None], Any] = _default_json_deserializer,
        value_deserializer: Callable[[bytes | None], Any] = _default_json_deserializer,
    ) -> None:
        self._config = config
        self._group_id = group_id
        self._auto_offset_reset = auto_offset_reset
        self._enable_auto_commit = enable_auto_commit

        self._key_serializer = (
            key_serializer if key_serializer is not None else self._json_serializer
        )
        self._value_serializer = (
            value_serializer if value_serializer is not None else self._json_serializer
        )
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer

        self._producer: Producer | None = None
        self._consumer: Consumer | None = None

        self._on_assign: (
            Callable[[Consumer, list[TopicPartition]], None] | None
        ) = None
        self._on_revoke: (
            Callable[[Consumer, list[TopicPartition]], None] | None
        ) = None

        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _json_serializer(obj: Any) -> tuple[bytes | None, str | None]:
        if obj is None:
            result = (None, None)

            return result

        serialized = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        result = (serialized, "application/json")

        return result

    def _ensure_producer(self) -> Producer:
        if self._producer is None:
            self._producer = Producer(self._config.producer_conf())

        return self._producer

    def _ensure_consumer(self) -> Consumer:
        if self._group_id is None:
            raise ValueError("group_id must be set to use consumer features")
        if self._consumer is None:
            conf: dict[str, Any] = {
                "bootstrap.servers": self._config.bootstrap_servers,
                "client.id": self._config.client_id,
                "group.id": self._group_id,
                "auto.offset.reset": self._auto_offset_reset,
                "enable.auto.commit": self._enable_auto_commit,
                "session.timeout.ms": 10000,
                "max.poll.interval.ms": 300000,
            }
            if self._config.kafka_debug:
                conf["debug"] = "cgrp,topic,broker,protocol"
                conf["statistics.interval.ms"] = 10000

            self._consumer = Consumer(conf)

            def _on_assign(
                consumer: Consumer,
                partitions: list[TopicPartition],
            ) -> None:
                self._logger.debug("Kafka on_assign: %s", partitions)
                if self._config.kafka_reset_from_beginning:
                    parts = [
                        TopicPartition(p.topic, p.partition, OFFSET_BEGINNING)
                        for p in partitions
                    ]
                    self._logger.warning(
                        "Resetting offsets to beginning for: %s", parts
                    )
                    consumer.assign(parts)
                else:
                    consumer.assign(partitions)

            def _on_revoke(
                consumer: Consumer,
                partitions: list[TopicPartition],
            ) -> None:
                self._logger.warning("Kafka on_revoke: %s", partitions)

            self._on_assign = _on_assign
            self._on_revoke = _on_revoke

        return self._consumer

    # ---------- Publisher ----------

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

    # ---------- Consumer ----------

    def subscribe(self, topics: Iterable[str]) -> None:
        consumer = self._ensure_consumer()
        consumer.subscribe(
            list(topics),
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
        )

    def poll_once(
        self,
        timeout: float = 1.0,
    ) -> tuple[Any, Any, dict[str, bytes], Message] | None:
        consumer = self._ensure_consumer()
        msg: Message | None = consumer.poll(timeout)
        if msg is None:
            result = None

            return result
        if msg.error():
            self._logger.warning("Kafka message error: %s", msg.error())
            result = None

            return result

        key = self._key_deserializer(msg.key())
        value = self._value_deserializer(msg.value())
        headers_list = msg.headers() or []
        headers = {k: (v if v is not None else b"") for k, v in headers_list}

        result = (key, value, headers, msg)

        return result

    def consume_forever(
        self,
        handler: Callable[[Any, Any, dict[str, bytes], Message], None],
        *,
        timeout: float = 1.0,
    ) -> None:
        _ = self._ensure_consumer()
        try:
            while True:
                item = self.poll_once(timeout)
                if not item:
                    continue
                key, value, headers, msg = item
                try:
                    handler(key, value, headers, msg)
                except Exception as e:
                    self._logger.exception("Handler error: %s", e)
        finally:
            self.close_consumer()

    def commit(
        self,
        msg: Message | None = None,
        *,
        asynchronous: bool = False,
    ) -> None:
        consumer = self._consumer
        if consumer is None:
            return
        try:
            consumer.commit(message=msg, asynchronous=asynchronous)
        except Exception as e:
            self._logger.error("Commit failed: %s", e)

    def close_consumer(self) -> None:
        consumer = self._consumer
        if consumer is not None:
            consumer.close()
            self._consumer = None

    def close_producer(self) -> None:
        producer = self._producer
        if producer is not None:
            producer.flush()
            self._producer = None

    # ---------- Introspection helpers ----------

    def list_topics(self, timeout: float = 10.0) -> Any:
        consumer = self._ensure_consumer()
        topics = consumer.list_topics(timeout=timeout)

        return topics

    def topic_partitions(self, topic: str, timeout: float = 10.0) -> list[int]:
        md = self.list_topics(timeout=timeout)
        if topic not in md.topics:
            result: list[int] = []

            return result
        tmd = md.topics[topic]
        if tmd.error is not None:
            self._logger.error("Topic '%s' metadata error: %s", topic, tmd.error)
            result = []

            return result
        partitions = sorted(p.id for p in tmd.partitions.values())

        return partitions

    def assignment(self) -> list[TopicPartition]:
        consumer = self._consumer
        if consumer is None:
            result: list[TopicPartition] = []

            return result
        assignment = consumer.assignment()

        return assignment


def get_kafka_client(
    config: KafkaConfig | None = None,
    group_id: str | None = None,
    **client_kwargs: Any,
) -> KafkaClient:
    cfg = config or get_kafka_config()

    client = KafkaClient(cfg, group_id, **client_kwargs)

    return client


def provide_kafka_client() -> KafkaClient:
    return get_kafka_client()
