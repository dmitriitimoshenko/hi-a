import logging
import uuid
from typing import Any

from confluent_kafka import Message

from app.config import Config
from app.kafka_client import KafkaClient


class KafkaEventHandler:
    def __init__(
        self,
        config: Config,
        kafka_client: KafkaClient,
    ) -> None:
        self._config = config
        self._kafka_client = kafka_client
        self._logger = logging.getLogger(__name__)

    def handle_hire_event(self, key: int, value: dict[str, Any]) -> None:
        topic = self._config.KAFKA_TOPIC_NOTIFICATION

        def _on_delivery(err: Exception | None, msg: Message) -> None:
            self._logger.info("kafka_publish_callback started")
            if err is not None:
                self._logger.error("Failed to publish message to Kafka: %s", err)
                return

            self._logger.info("Successfully published message to Kafka: %s", msg)

        payload = self._mask_embedding(value)

        self._logger.info("Sending this to kafka (key: %s): %s", key, payload)

        self._kafka_client.publish(
            topic=topic,
            key=key,
            value=payload,
            on_delivery=_on_delivery,
        )

        self._kafka_client.flush(5.0)

    def _mask_embedding(self, payload: dict[str, Any]) -> dict[str, Any]:
        masked = payload.copy()
        try:
            mapped_application = masked.get("mapped_application")
            if isinstance(mapped_application, dict) and "embedding" in mapped_application:
                mapped_application = mapped_application.copy()
                mapped_application["embedding"] = "hidden"
                masked["mapped_application"] = mapped_application
        except Exception:
            self._logger.warning("Failed to hide embedding field for kafka payload")

        result = masked

        return result

    def handle_application_sync(self, value: dict[str, Any]) -> None:
        topic = self._config.KAFKA_TOPIC_NOTIFICATION_SYNC

        if not topic:
            self._logger.error("KAFKA_TOPIC_NOTIFICATION_SYNC is not configured")

            return

        if not isinstance(value, dict):
            self._logger.error(
                "Invalid payload type for applications-sync-processed message: %s",
                value,
            )

            return

        application_id = value.get("application_id")
        key = application_id if application_id is not None else value.get("row_id")

        if key is None:
            self._logger.error(
                "Cannot determine kafka key for applications-sync-processed payload: %s",
                value,
            )

            return

        event_id = str(uuid.uuid4())
        payload = {
            "event_id": event_id,
            "type": "application_diff",
            "application_id": application_id,
            "row_id": value.get("row_id"),
            "company": value.get("company"),
            "role_title": value.get("role_title"),
            "differences": value.get("differences", []),
            "sheet_payload": value.get("sheet_payload", {}),
            "db_snapshot": value.get("db_snapshot"),
            "errors": value.get("errors", []),
            "sheet_id": value.get("sheet_id"),
            "sheet_page": value.get("sheet_page"),
            "range": value.get("range"),
            "detected_at": value.get("detected_at"),
            "received_at": self._utc_now_iso(),
        }

        def _on_delivery(err: Exception | None, msg: Message) -> None:
            if err is not None:
                self._logger.error(
                    "Failed to publish notification-sync event (event_id=%s): %s",
                    event_id,
                    err,
                )

                return

            self._logger.info(
                "Published notification-sync event (event_id=%s, topic=%s, partition=%s, offset=%s)",
                event_id,
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

        self._kafka_client.publish(
            topic=topic,
            key=key,
            value=payload,
            on_delivery=_on_delivery,
        )

        self._kafka_client.flush(5.0)

    def _utc_now_iso(self) -> str:
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc).replace(microsecond=0)
        iso = now.isoformat()

        if iso.endswith("+00:00"):
            normalized = f"{iso[:-6]}Z"

            return normalized

        return iso


def get_kafka_event_handler(
    config: Config,
    kafka_client: KafkaClient,
) -> KafkaEventHandler:
    handler = KafkaEventHandler(
        config=config,
        kafka_client=kafka_client,
    )

    return handler
