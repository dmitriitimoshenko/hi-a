import json
import logging
import socket

from collections import deque
from dataclasses import dataclass
from typing import Any, Callable, Iterable

import redis
from redis.exceptions import ResponseError

from .config import BusConfig, get_bus_config

logging.basicConfig(level=logging.INFO)

PENDING_START_ID = "0"
NEW_MESSAGES_ID = ">"

MESSAGE_FIELD_KEY = b"key"
MESSAGE_FIELD_VALUE = b"value"


def _default_json_deserializer(payload: bytes | None) -> Any:
    if payload is None:
        result = None

        return result

    try:
        result = json.loads(payload.decode("utf-8"))
    except Exception:
        result = payload

    return result


def _default_json_serializer(obj: Any) -> bytes | None:
    if obj is None:
        result = None

        return result

    result = json.dumps(obj, ensure_ascii=False).encode("utf-8")

    return result


@dataclass(slots=True)
class BusMessage:
    topic: str
    entry_id: str
    key: Any
    value: Any


class BusClient:
    """Thin wrapper over Redis Streams providing at-least-once consumer groups."""

    def __init__(
        self,
        config: BusConfig,
        group_id: str | None = None,
        *,
        key_serializer: Callable[[Any], bytes | None] = _default_json_serializer,
        value_serializer: Callable[[Any], bytes | None] = _default_json_serializer,
        key_deserializer: Callable[[bytes | None], Any] = _default_json_deserializer,
        value_deserializer: Callable[[bytes | None], Any] = _default_json_deserializer,
    ) -> None:
        if not config.redis_url:
            raise ValueError("REDIS_URL is not set")

        self._config = config
        self._group_id = group_id

        self._key_serializer = key_serializer
        self._value_serializer = value_serializer
        self._key_deserializer = key_deserializer
        self._value_deserializer = value_deserializer

        self._consumer_id = config.consumer_id or socket.gethostname()
        self._client: redis.Redis | None = None
        self._topics: list[str] = []
        self._buffer: deque[BusMessage] = deque()

        # Entries already delivered to this consumer but never acknowledged are
        # replayed first, so a crash mid-handler does not lose the message.
        self._read_id = PENDING_START_ID

        self._logger = logging.getLogger(__name__)

    def _ensure_client(self) -> redis.Redis:
        if self._client is None:
            self._client = redis.Redis.from_url(
                self._config.redis_url,
                db=self._config.db,
                decode_responses=False,
            )

        return self._client

    # ---------- Publisher ----------

    def publish(self, topic: str, key: Any = None, value: Any = None) -> None:
        client = self._ensure_client()

        fields: dict[bytes, bytes] = {
            MESSAGE_FIELD_KEY: self._key_serializer(key) or b"",
            MESSAGE_FIELD_VALUE: self._value_serializer(value) or b"",
        }

        try:
            client.xadd(
                name=topic,
                fields=fields,
                maxlen=self._config.max_len,
                approximate=True,
            )
        except Exception as e:
            self._logger.error("Bus publish to %s failed: %s", topic, e)

            raise

        self._logger.debug("Bus delivered to %s", topic)

    # ---------- Consumer ----------

    def subscribe(self, topics: Iterable[str]) -> None:
        if self._group_id is None:
            raise ValueError("group_id must be set to use consumer features")

        client = self._ensure_client()

        self._topics = list(topics)
        start_id = PENDING_START_ID if self._config.start_at_oldest else "$"

        for topic in self._topics:
            try:
                client.xgroup_create(
                    name=topic,
                    groupname=self._group_id,
                    id=start_id,
                    mkstream=True,
                )
            except ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    raise

        self._read_id = PENDING_START_ID

    def poll_once(self, timeout: float = 1.0) -> BusMessage | None:
        if self._buffer:
            buffered = self._buffer.popleft()

            return buffered

        self._fill_buffer(timeout)

        if not self._buffer:
            return None

        message = self._buffer.popleft()

        return message

    def commit(self, message: BusMessage | None = None) -> None:
        if message is None or self._group_id is None:
            return

        client = self._ensure_client()

        try:
            client.xack(message.topic, self._group_id, message.entry_id)
        except Exception as e:
            self._logger.error("Bus ack failed for %s: %s", message.topic, e)

    def close(self) -> None:
        client = self._client
        if client is not None:
            client.close()
            self._client = None

    def _fill_buffer(self, timeout: float) -> None:
        client = self._ensure_client()

        if not self._topics:
            return

        # Two passes at most: the recovery phase reads the pending list, and
        # once it is drained the very same call switches to new messages, so a
        # poll never comes back empty merely because recovery finished.
        for _ in range(2):
            response = client.xreadgroup(
                groupname=self._group_id,
                consumername=self._consumer_id,
                streams={topic: self._read_id for topic in self._topics},
                count=self._config.batch_count,
                block=int(timeout * 1000),
            )

            entries = self._collect_entries(response)

            if entries:
                # While replaying the pending list, page forward by the last
                # seen ID. Entries whose handler fails stay unacknowledged and
                # are skipped for this run instead of being re-read in a tight
                # loop; they come back on the next restart.
                if self._read_id != NEW_MESSAGES_ID:
                    self._read_id = entries[-1].entry_id

                self._buffer.extend(entries)

                return

            if self._read_id == NEW_MESSAGES_ID:
                return

            self._read_id = NEW_MESSAGES_ID

    def _collect_entries(self, response: Any) -> list[BusMessage]:
        entries: list[BusMessage] = []

        for stream_name, stream_entries in response or []:
            topic = (
                stream_name.decode("utf-8")
                if isinstance(stream_name, bytes)
                else stream_name
            )

            for entry_id, fields in stream_entries:
                message = BusMessage(
                    topic=topic,
                    entry_id=(
                        entry_id.decode("utf-8")
                        if isinstance(entry_id, bytes)
                        else entry_id
                    ),
                    key=self._key_deserializer(fields.get(MESSAGE_FIELD_KEY)),
                    value=self._value_deserializer(fields.get(MESSAGE_FIELD_VALUE)),
                )

                entries.append(message)

        return entries


def get_bus_client(
    config: BusConfig | None = None,
    group_id: str | None = None,
    **client_kwargs: Any,
) -> BusClient:
    cfg = config or get_bus_config()

    client = BusClient(cfg, group_id, **client_kwargs)

    return client


def provide_bus_client() -> BusClient:
    client = get_bus_client()

    return client
