import logging
import re
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Message

from app.config import Config
from app.kafka_client import KafkaClient

MEETING_CONFIRMATION_LABELS = {"meeting_crt", "meeting_inv"}
MEETING_CREATION_LABEL = "meeting_crt"


class KafkaEventHandler:
    def __init__(
        self,
        config: Config,
        kafka_client: KafkaClient,
    ) -> None:
        self._config = config
        self._kafka_client = kafka_client
        self._logger = logging.getLogger(__name__)

    def handle_interesting_mail(self, key: int, value: dict[str, Any]) -> None:
        topic = self._config.KAFKA_TOPIC_HIRE_EVENT

        def _on_delivery(err: Exception | None, msg: Message) -> None:
            self._logger.info("kafka_publish_callback started")
            if err is not None:
                self._logger.error("Failed to publish message to Kafka: %s", err)

                return

            self._logger.info("Successfully published message to Kafka: %s", msg)

        self._logger.info("Sending this to kafka: %s", value)

        self._kafka_client.publish(
            topic=topic,
            key=key,
            value=value,
            on_delivery=_on_delivery,
        )

        self._kafka_client.flush(5.0)

    def handle_application_update(self, key: Any, value: dict[str, Any]) -> None:
        self._apply_denied_confirmation(value)
        self._update_stage_if_needed(value)
        self._enrich_meeting_confirmation(value)

        if not isinstance(value, dict):
            self._logger.error(
                "Invalid APPLICATION_UPDATE payload type: %s",
                type(value),
            )

            return

        topic = self._config.KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED
        if not topic:
            self._logger.error("KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED is not set")

            return

        def _on_delivery(err: Exception | None, msg: Message) -> None:
            self._logger.info("kafka_publish_callback started")
            if err is not None:
                self._logger.error("Failed to publish message to Kafka: %s", err)

                return

            self._logger.info("Successfully published message to Kafka: %s", msg)

        self._logger.info(
            "Forwarding application update to kafka topic=%s: %s",
            topic,
            value,
        )

        self._kafka_client.publish(
            topic=topic,
            key=key,
            value=value,
            on_delivery=_on_delivery,
        )

        self._kafka_client.flush(5.0)

    def _apply_denied_confirmation(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        if not self._is_denied_confirmation(payload):
            return

        mapped_application = payload.get("mapped_application")
        if not isinstance(mapped_application, dict):
            self._logger.warning(
                "Skipping mapped_application update for denied confirmation: missing or invalid",
            )
            mapped_application = None

        payload["status"] = "denied"
        if mapped_application is not None:
            mapped_application["status"] = "denied"

        responded_at = payload.get("responded_at")
        if mapped_application is not None and not responded_at:
            responded_at = mapped_application.get("responded_at")

        if responded_at:
            return

        now = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        payload["responded_at"] = now
        if mapped_application is not None:
            mapped_application["responded_at"] = now

    def _update_stage_if_needed(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        if not self._is_meeting_creation_confirmation(payload):
            return

        mapped_application = payload.get("mapped_application")
        if not isinstance(mapped_application, dict):
            self._logger.warning(
                "Skipping stage update: mapped_application is missing or invalid",
            )

            return

        application_id = mapped_application.get("id")
        row_id = mapped_application.get("row_id")
        if application_id is None or row_id is None:
            self._logger.warning(
                "Skipping stage update: application_id or row_id is missing "
                "(application_id=%s, row_id=%s)",
                application_id,
                row_id,
            )

            return

        current_stage = self._parse_stage(mapped_application.get("stage"))
        next_stage = current_stage + 1
        mapped_application["stage"] = next_stage
        payload["stage"] = next_stage

    def _is_meeting_creation_confirmation(self, payload: dict[str, Any]) -> bool:
        label = self._get_email_label(payload)
        action = str(payload.get("action") or "").lower()

        if label != MEETING_CREATION_LABEL:
            return False

        if action and action != "cnfm":
            return False

        return True

    def _is_meeting_confirmation(self, payload: dict[str, Any]) -> bool:
        label = self._get_email_label(payload)
        action = str(payload.get("action") or "").lower()

        if label not in MEETING_CONFIRMATION_LABELS:
            return False

        if action and action != "cnfm":
            return False

        return True

    def _get_email_label(self, payload: dict[str, Any]) -> str:
        email = payload.get("email") or {}

        return str(email.get("label") or "").lower()

    def _parse_stage(self, value: Any) -> int:
        if value is None:
            return 0

        try:
            return int(str(value).strip())
        except Exception:
            self._logger.warning("Invalid stage value '%s', defaulting to 0", value)

            return 0

    def _enrich_meeting_confirmation(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        if not self._is_meeting_confirmation(payload):
            return

        mapped_application = payload.get("mapped_application")
        if not isinstance(mapped_application, dict):
            self._logger.warning(
                "Skipping meeting_crt enrichment: mapped_application is missing or invalid",
            )

            return

        self._ensure_meeting_status(payload, mapped_application)
        self._ensure_responded_at(payload, mapped_application)
        self._ensure_next_follow_up(payload, mapped_application)

    def _ensure_meeting_status(
        self,
        payload: dict[str, Any],
        mapped_application: dict[str, Any],
    ) -> None:
        status_value = mapped_application.get("status")

        if not self._should_set_meeting_status(status_value):
            return

        mapped_application["status"] = "meeting"
        payload["status"] = "meeting"

    def _ensure_responded_at(
        self,
        payload: dict[str, Any],
        mapped_application: dict[str, Any],
    ) -> None:
        responded_at = mapped_application.get("responded_at")
        if responded_at:
            return

        now = datetime.now(timezone.utc).replace(microsecond=0)
        responded_iso = now.isoformat()

        mapped_application["responded_at"] = responded_iso
        payload["responded_at"] = responded_iso

    def _ensure_next_follow_up(
        self,
        payload: dict[str, Any],
        mapped_application: dict[str, Any],
    ) -> None:
        if self._get_email_label(payload) != MEETING_CREATION_LABEL:
            return

        if mapped_application.get("next_follow_up_at"):
            return

        email = payload.get("email") or {}
        if not isinstance(email, dict):
            return

        meeting_dt = self._extract_meeting_datetime(email)
        if meeting_dt is None:
            self._logger.info("No meeting datetime extracted from email id=%s", email.get("id"))
            return

        meeting_iso = meeting_dt.replace(microsecond=0).isoformat()

        mapped_application["next_follow_up_at"] = meeting_iso
        payload["next_follow_up_at"] = meeting_iso
        self._logger.info(
            "Enriched next_follow_up_at from ICS: email_id=%s value=%s",
            email.get("id"),
            meeting_iso,
        )

    def _should_set_meeting_status(self, status_value: Any) -> bool:
        if status_value is None:
            return True

        try:
            normalized = str(status_value).strip().lower()
        except Exception:
            return True

        forbidden = {"meeting", "offer", "denied"}

        return normalized not in forbidden

    def _extract_meeting_datetime(self, email: dict[str, Any]) -> datetime | None:
        ics_list = []
        try:
            ics_list = email.get("ics_file_data_list") or email.get("ics_files") or []
        except Exception:
            ics_list = []

        for item in ics_list:
            if not isinstance(item, dict):
                continue

            content = item.get("content") or ""
            meeting_dt = self._parse_dtstart(content)
            if meeting_dt is not None:
                return meeting_dt

        return None

    def _parse_dtstart(self, content: str) -> datetime | None:
        if not content:
            return None

        match = re.search(
            r"DTSTART(?:;TZID=([^:]+))?:([0-9]{8}T[0-9]{6})(Z)?",
            content,
            re.IGNORECASE,
        )
        if match is None:
            self._logger.info("DTSTART not found in ICS content")
            return None

        tzid = match.group(1)
        timestamp = match.group(2)
        tz_suffix = match.group(3) or ""

        try:
            dt = datetime.strptime(timestamp, "%Y%m%dT%H%M%S")
        except Exception:
            self._logger.warning("Failed to parse timestamp '%s' from ICS", timestamp)
            return None

        if tz_suffix == "Z":
            dt = dt.replace(tzinfo=timezone.utc)
        elif tzid:
            try:
                from zoneinfo import ZoneInfo

                dt = dt.replace(tzinfo=ZoneInfo(tzid))
            except Exception:
                self._logger.warning("Failed to apply timezone '%s' for DTSTART", tzid)

        return dt

    def handle_application_sync(self, key: Any, value: dict[str, Any]) -> None:
        topic = self._config.KAFKA_TOPIC_APPLICATIONS_SYNC_PROCESSED

        if not topic:
            self._logger.error(
                "KAFKA_TOPIC_APPLICATIONS_SYNC_PROCESSED is not configured, cannot forward application diff",
            )

            return

        self._logger.info(
            "Forwarding application diff to processed topic=%s key=%s",
            topic,
            key,
        )

        self._kafka_client.publish(
            topic=topic,
            key=key,
            value=value,
        )

        self._kafka_client.flush(5.0)

    def _is_denied_confirmation(self, payload: dict[str, Any]) -> bool:
        email = payload.get("email") or {}
        action = str(payload.get("action") or "").lower()
        label = str(email.get("label") or "").lower()

        if label != "denied":
            return False

        if action and action != "cnfm":
            return False

        return True


def get_kafka_event_handler(
    config: Config,
    kafka_client: KafkaClient,
) -> KafkaEventHandler:
    handler = KafkaEventHandler(
        config=config,
        kafka_client=kafka_client,
    )

    return handler
