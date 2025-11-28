from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from app.enums import FeedbackReason
from app.services.feedback import FeedbackPayload, MailMappingFeedbackService


@dataclass
class InboundFeedbackMessage:
    email_id: int
    application_id: int | None
    reason: FeedbackReason
    user_email: str | None
    telegram_user_id: int | None
    telegram_username: str | None
    source: str
    payload: dict[str, Any] | None
    feedback_created_at: datetime | None


class KafkaFeedbackHandler:
    def __init__(self, feedback_service: MailMappingFeedbackService) -> None:
        self._feedback_service = feedback_service
        self._logger = logging.getLogger(__name__)

    def handle(self, value: dict[str, Any]) -> None:
        if not isinstance(value, dict):
            self._logger.error(
                "Feedback payload has invalid type: %s",
                type(value).__name__,
            )

            return

        try:
            message = self._parse_message(value)
        except ValueError as e:
            self._logger.error("Failed to parse feedback payload: %s", e)

            return

        payload = FeedbackPayload(
            email_id=message.email_id,
            application_id=message.application_id,
            reason=message.reason,
            user_email=message.user_email,
            telegram_user_id=message.telegram_user_id,
            telegram_username=message.telegram_username,
            source=message.source,
            payload=message.payload,
            feedback_created_at=message.feedback_created_at,
        )

        self._feedback_service.record_feedback(payload)

    def _parse_message(self, data: dict[str, Any]) -> InboundFeedbackMessage:
        email_id_raw = data.get("email_id")
        if email_id_raw is None:
            raise ValueError("email_id is missing")
        try:
            email_id = int(email_id_raw)
        except (TypeError, ValueError) as e:
            raise ValueError(f"email_id has invalid value: {email_id_raw}") from e

        application_id_raw = data.get("application_id")
        application_id: int | None
        if application_id_raw is None or application_id_raw == "":
            application_id = None
        else:
            try:
                application_id = int(application_id_raw)
            except (TypeError, ValueError) as e:
                raise ValueError(
                    f"application_id has invalid value: {application_id_raw}"
                ) from e

        reason_raw = data.get("reason")
        if not isinstance(reason_raw, str):
            raise ValueError("reason is missing or not a string")
        try:
            reason = FeedbackReason(reason_raw)
        except ValueError as e:
            raise ValueError(f"Unsupported feedback reason: {reason_raw}") from e

        user_email = data.get("user_email")
        if not isinstance(user_email, str):
            user_email = None

        telegram_user = data.get("telegram_user")
        telegram_user_id: int | None = None
        telegram_username: str | None = None
        if isinstance(telegram_user, dict):
            telegram_user_id_raw = telegram_user.get("id")
            try:
                telegram_user_id = int(telegram_user_id_raw)
            except (TypeError, ValueError):
                telegram_user_id = None

            username_raw = telegram_user.get("username")
            if isinstance(username_raw, str) and username_raw:
                telegram_username = username_raw

        source_raw = data.get("source")
        source = source_raw if isinstance(source_raw, str) and source_raw else "unknown"

        payload_raw = data.get("payload")
        payload: dict[str, Any] | None
        if isinstance(payload_raw, dict):
            payload = payload_raw
        else:
            payload = None

        created_at_raw = data.get("created_at") or data.get("feedback_created_at")
        feedback_created_at: datetime | None = None
        if isinstance(created_at_raw, str) and created_at_raw:
            normalized = created_at_raw.replace("Z", "+00:00")
            try:
                feedback_created_at = datetime.fromisoformat(normalized)
            except ValueError:
                self._logger.warning(
                    "Failed to parse feedback_created_at value: %s",
                    created_at_raw,
                )

        message = InboundFeedbackMessage(
            email_id=email_id,
            application_id=application_id,
            reason=reason,
            user_email=user_email,
            telegram_user_id=telegram_user_id,
            telegram_username=telegram_username,
            source=source,
            payload=payload,
            feedback_created_at=feedback_created_at,
        )

        return message


def get_kafka_feedback_handler(
    feedback_service: MailMappingFeedbackService,
) -> KafkaFeedbackHandler:
    handler = KafkaFeedbackHandler(feedback_service)

    return handler
