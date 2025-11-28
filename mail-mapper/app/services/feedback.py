from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable, ContextManager

from sqlalchemy.orm import Session

from app.database import get_db_context
from app.enums import FeedbackReason
from app.models import MailMappingFeedback, MailMappingMatch

DbSessionFactory = Callable[[], ContextManager[Session]]


@dataclass
class FeedbackPayload:
    email_id: int
    application_id: int | None
    reason: FeedbackReason
    user_email: str | None
    telegram_user_id: int | None
    telegram_username: str | None
    source: str
    payload: dict[str, Any] | None
    feedback_created_at: datetime | None


class MailMappingFeedbackService:
    def __init__(self, db_factory: DbSessionFactory) -> None:
        self._db_factory = db_factory
        self._logger = logging.getLogger(__name__)

    def record_feedback(self, payload: FeedbackPayload) -> None:
        with self._db_factory() as db:
            match_row = self._resolve_match(
                db=db,
                email_id=payload.email_id,
                application_id=payload.application_id,
            )

            feedback = MailMappingFeedback(
                feedback_created_at=payload.feedback_created_at,
                email_id=payload.email_id,
                application_id=payload.application_id,
                reason=payload.reason.value,
                user_email=payload.user_email,
                telegram_user_id=payload.telegram_user_id,
                telegram_username=payload.telegram_username,
                source=payload.source,
                payload=payload.payload,
            )

            if match_row is not None:
                feedback.match_id = match_row.id
                feedback.matching_batch_id = match_row.matching_batch_id

            db.add(feedback)
            db.commit()

            self._logger.info(
                "Recorded feedback for email_id=%d application_id=%s reason=%s config_version=%s",
                payload.email_id,
                payload.application_id,
                payload.reason.value,
                match_row.scoring_config_version if match_row else None,
            )

    def _resolve_match(
        self,
        *,
        db: Session,
        email_id: int,
        application_id: int | None,
    ) -> MailMappingMatch | None:
        query = db.query(MailMappingMatch).where(
            MailMappingMatch.email_id == email_id
        )

        if application_id is not None:
            query = query.where(MailMappingMatch.application_id == application_id)

        match_row = query.order_by(MailMappingMatch.created_at.desc()).first()

        return match_row


def get_mail_mapping_feedback_service(
    db_factory: DbSessionFactory | None = None,
) -> MailMappingFeedbackService:
    factory = db_factory or get_db_context

    service = MailMappingFeedbackService(db_factory=factory)

    return service
