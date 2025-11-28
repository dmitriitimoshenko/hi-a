import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo
from fastapi import Depends
from sqlalchemy.orm import Session

from app.config import Config
from app.database import get_db, get_db_context
from app.enums import EmailLabel
from app.integrations.open_ai.service.service import (
    OpenAIService,
    provide_openai_service,
)
from app.kafka_client.client import (
    KafkaClient,
    provide_kafka_client,
)
from app.mail_mapper_client import (
    MailMapperClient,
    MailMapperEmailPayload,
    MailMapperMatch,
    get_mail_mapper_client,
)
from app.models import EmbdLrn, IcsFileData
from app.tools.normalization import normalize_content
from app.services.embd_lrn import EmbdLrnService, get_embd_service

logger = logging.getLogger(__name__)
CET_ZONE = ZoneInfo("CET")


@dataclass
class EmailToApplicationMappingDTO:
    email: dict[str, Any]
    mapped_application: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "email": self.email,
            "mapped_application": self.mapped_application,
        }

        return payload


class MailService:
    def __init__(
        self,
        db: Session,
        kafka_client: KafkaClient,
        openai_service: OpenAIService,
        *,
        mapper_client: MailMapperClient | None = None,
        config: Config | None = None,
    ) -> None:
        self._db = db
        self._kafka_client = kafka_client
        self._openai_service = openai_service
        self._mapper_client = mapper_client or get_mail_mapper_client()
        self._config = config or Config()
        self._embd_service = get_embd_service(
            db=self._db,
            openai_service=self._openai_service,
        )
        self._logger = logging.getLogger(__name__)

        self._interesting_mail_topic = self._config.KAFKA_TOPIC_INTERESTING_MAIL or ""
        if not self._interesting_mail_topic:
            message = "KAFKA_TOPIC_INTERESTING_MAIL value is not set"
            self._logger.error(message)
            raise ValueError(message)

    def submit_interesting_of_status(
        self,
        status: EmailLabel,
    ) -> tuple[list[EmailToApplicationMappingDTO] | None, int, int]:
        self._logger.info(
            "submit_interesting_of_status started for status: %s",
            status,
        )

        try:
            emails = self._get_emails_not_checked_for_interest(status)
        except Exception as e:
            message = f"Failed to get emails not checked for interest: {e}"
            self._logger.error(message)
            raise ValueError(message)

        if not emails:
            self._logger.info("No new emails to check for interest")
            return None, 0, 0

        mappings: list[EmailToApplicationMappingDTO] = []
        skipped_emails_amount = 0
        submitted_emails_amount = 0

        try:
            batch_mappings, batch_skipped = self._process_email_group(
                status,
                emails,
            )
        except Exception as e:
            message = f"Failed to process email batch: {e}"
            self._logger.error(message)
            raise ValueError(message)

        skipped_emails_amount += batch_skipped

        for email_instance, mapping in batch_mappings:
            mappings.append(mapping)
            self._logger.info("Sending this to kafka: %s", mapping)
            self._publish_mapping(email_instance, mapping)
            submitted_emails_amount += 1

        result_mappings: list[EmailToApplicationMappingDTO] | None = (
            mappings if mappings else None
        )

        return result_mappings, skipped_emails_amount, submitted_emails_amount

    def submit_interesting(
        self,
    ) -> dict[EmailLabel, tuple[list[EmailToApplicationMappingDTO] | None, int, int]]:
        labels_to_submit = [
            EmailLabel.APPLIED,
            EmailLabel.DENIED,
            EmailLabel.MEETING_INV,
            EmailLabel.MEETING_CRT,
            EmailLabel.MEETING_UPD,
            EmailLabel.MEETING_CNCL,
            EmailLabel.OFFER,
        ]

        results: dict[
            EmailLabel,
            tuple[list[EmailToApplicationMappingDTO] | None, int, int],
        ] = {}

        for label in labels_to_submit:
            try:
                results[label] = self.submit_interesting_of_status(label)
            except Exception as e:
                message = (
                    f"Failed to submit interesting emails for label {label}: {e}"
                )
                self._logger.error(message)
                results[label] = (None, 0, 0)

        return results

    def create_with_label(self, items: list[dict], label: EmailLabel) -> None:
        items_to_save: list[EmbdLrn] = []

        for item in items:
            if not isinstance(item, list) or len(item) != 4:
                continue

            sender_email = item[0]
            sender_name = item[1]
            subject = item[2]
            body = item[3]

            content = f"SUBJECT {subject} BODY {body}"

            items_to_save.append(
                EmbdLrn(
                    label=label,
                    subject=subject,
                    body=body,
                    content=content,
                    sender_email=sender_email,
                    sender_name=sender_name,
                    should_be_sent_to_heh=False,
                )
            )

        try:
            self._db.add_all(items_to_save)
            self._db.commit()
            self._logger.debug(
                "Saved items on create_with_label: %d",
                len(items_to_save),
            )
        except Exception as e:
            message = f"Failed to create_with_label: {e}"
            self._logger.debug(message)
            raise ValueError(message)

    def learn(self) -> None:
        try:
            items = (
                self._db.query(EmbdLrn).where(EmbdLrn.used_for_learning.is_(None)).all()
            )
            if not items:
                message = "No EmbdLrn not used_for_learning found"
                self._logger.warning(message)
                return
        except Exception as e:
            message = "Failed to get EmbdLrn not used_for_learning"
            self._logger.error(message)
            raise ValueError(message) from e

        try:
            contents = [normalize_content(item.content) for item in items]

            embeddings = self._openai_service.get_embeddings(contents)
        except Exception as e:
            message = f"Error during learning: {e}"
            self._logger.error(message)
            raise ValueError(message)

        try:
            for item, embedding in zip(items, embeddings, strict=False):
                item.embedding = embedding
                self._db.add(item)

            self._db.commit()

            self._logger.debug(
                "Received embeddings for %d items of %d tokens",
                len(embeddings),
                len(embeddings[0]) if embeddings else 0,
            )
        except Exception:
            message = "Failed to save a list of EmbdLrn with update embeddings"
            self._logger.error(message)
            raise ValueError(message)

    def commit(self) -> None:
        self._embd_service.commit()

    def _process_email_group(
        self,
        status: EmailLabel,
        emails: list[EmbdLrn],
    ) -> tuple[list[tuple[EmbdLrn, EmailToApplicationMappingDTO]], int]:
        payloads: list[MailMapperEmailPayload] = []

        for email in emails:
            if email.embedding is None:
                message = f"Email with id: {email.id} has no embedding assigned"
                self._logger.error(message)
                raise ValueError(message)

            try:
                email_label = EmailLabel(email.label)
            except ValueError as e:
                message = f"Unsupported label on email id {email.id}: {email.label}"
                self._logger.error(message)
                raise ValueError(message) from e

            payloads.append(
                MailMapperEmailPayload(
                    id=email.id,
                    label=email_label,
                    created_at=email.created_at,
                    sender_email=email.sender_email,
                    sender_name=email.sender_name,
                    embedding=email.embedding,
                )
            )

        try:
            mapper_result = self._mapper_client.resolve_mappings(
                status=status,
                emails=payloads,
            )
        except Exception as e:
            message = f"Failed to resolve mappings via mail-mapper: {e}"
            self._logger.error(message)
            raise ValueError(message)

        matches_by_email_id: dict[int, MailMapperMatch] = {
            match.email_id: match for match in mapper_result.matches
        }

        mappings: list[tuple[EmbdLrn, EmailToApplicationMappingDTO]] = []
        skipped = 0

        for email in emails:
            match = matches_by_email_id.get(email.id)
            if match is None:
                self._logger.info(
                    "Email id %d could not be mapped to any application",
                    email.id,
                )
                skipped += 1
                continue

            email_dict = email.to_dict()
            email_dict["ics_file_data_list"] = self._fetch_ics_payload(email.id)

            mapping = EmailToApplicationMappingDTO(
                email=email_dict,
                mapped_application=match.application,
            )
            mappings.append((email, mapping))

        if mapper_result.skipped_email_ids:
            self._logger.info(
                "Mail-mapper skipped email ids: %s",
                mapper_result.skipped_email_ids,
            )

        return mappings, skipped

    def _fetch_ics_payload(self, email_id: int) -> list[dict[str, Any]] | None:
        try:
            records = (
                self._db.query(IcsFileData)
                .where(IcsFileData.embd_lrn_id == email_id)
                .all()
            )
        except Exception as e:
            message = f"Looking for ICS files failed {e}"
            self._logger.error(message)
            raise ValueError(message)

        if not records:
            return None

        payload = [record.to_dict() for record in records]

        return payload

    def _publish_mapping(
        self,
        email: EmbdLrn,
        mapping: EmailToApplicationMappingDTO,
    ) -> None:
        def _on_delivery(err, msg) -> None:
            self._logger.info("kafka_publish_callback started")
            if err:
                self._logger.error("Failed to publish message to Kafka: %s", err)
                return

            self._logger.info("Successfully published message to Kafka: %s", msg)
            self._mark_email_as_sent(email.id)

        self._kafka_client.publish(
            self._interesting_mail_topic,
            email.id,
            mapping.to_dict(),
            on_delivery=_on_delivery,
        )

        self._kafka_client.flush(5.0)

    def _mark_email_as_sent(self, email_id: int) -> None:
        try:
            with get_db_context() as db:
                instance = db.get(EmbdLrn, email_id)
                if instance is None:
                    self._logger.warning(
                        "Email with id %d not found when marking as sent",
                        email_id,
                    )
                    return

                instance.should_be_sent_to_heh = False
                db.add(instance)
                db.commit()
        except Exception:
            self._logger.exception(
                "Database error on updating email id: %d",
                email_id,
            )

    def _get_emails_not_checked_for_interest(
        self,
        label: EmailLabel,
    ) -> list[EmbdLrn]:
        oldest_delta, newest_delta = self._resolve_time_window(label)

        try:
            items = (
                self._db.query(EmbdLrn)
                .where(
                    EmbdLrn.should_be_sent_to_heh.is_(True),
                    EmbdLrn.label == label,
                    EmbdLrn.created_at
                    >= datetime.now(tz=CET_ZONE) - oldest_delta,
                    EmbdLrn.created_at
                    <= datetime.now(tz=CET_ZONE) - newest_delta,
                )
                .all()
            )
        except Exception as e:
            self._logger.error("Database query error: %s", e)
            return []

        return items

    def _resolve_time_window(
        self,
        label: EmailLabel,
    ) -> tuple[timedelta, timedelta]:
        if label == EmailLabel.APPLIED:
            return timedelta(hours=5), timedelta(minutes=30)

        if label == EmailLabel.DENIED:
            return timedelta(days=30), timedelta(minutes=30)

        if label in (
            EmailLabel.MEETING_INV,
            EmailLabel.MEETING_CRT,
            EmailLabel.MEETING_UPD,
            EmailLabel.MEETING_CNCL,
        ):
            return timedelta(days=30), timedelta(minutes=30)

        if label == EmailLabel.OFFER:
            return timedelta(days=60), timedelta(days=1)

        message = f"Unsupported label for _get_emails_not_checked_for_interest: {label}"
        raise ValueError(message)


def get_mail_service(
    db: Session = Depends(get_db),
    kafka_client: KafkaClient = Depends(provide_kafka_client),
    openai_service: OpenAIService = Depends(provide_openai_service),
    mapper_client: MailMapperClient = Depends(get_mail_mapper_client),
) -> MailService:
    config = Config()

    return MailService(
        db,
        kafka_client,
        openai_service,
        mapper_client=mapper_client,
        config=config,
    )
