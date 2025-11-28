import logging
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, ContextManager, Iterable

from sqlalchemy.orm import Session

from app.config import Config
from app.database import get_db_context
from app.enums import EmailLabel
from app.integrations.open_ai.service import OpenAIService, get_openai_service
from app.models import EmbdCntr, EmbdLrn, IcsFileData
from app.tools.normalization import normalize_content
from app.thresholds import resolve_threshold

DbSessionFactory = Callable[[], ContextManager[Session]]


@dataclass
class InboundIcsFile:
    filename: str
    content_type: str
    disposition: str | None
    method: str | None
    size: int
    content: str | None


@dataclass
class InboundMailMessage:
    subject: str
    body: str
    sender_email: str
    sender_name: str
    recipient_name: str | None
    recipient_email: str | None
    content_type: str | None
    ics_files: list[InboundIcsFile]


@dataclass
class ClassifiedEmail:
    label: EmailLabel
    raw_label: str
    cluster_id: int
    score: float
    threshold: float
    threshold_source: str
    top_two_delta: float | None
    could_be: str | None
    scores: dict[str, dict[int, float]]


@dataclass
class ClusterCentroid:
    label: str
    cluster_id: int
    embedding: list[float]
    threshold: float | None


class KafkaNewMailHandler:
    def __init__(
        self,
        config: Config,
        openai_service: OpenAIService,
        db_factory: DbSessionFactory,
    ) -> None:
        self._config = config
        self._openai_service = openai_service
        self._db_factory = db_factory
        self._logger = logging.getLogger(__name__)

    def handle(self, key: int, value: dict[str, Any]) -> None:
        message = self._parse_mail_message(value)

        content = self._build_content(message.subject, message.body)

        self._logger.info("Content after normalization: %s", content)

        embedding = self._openai_service.get_embedding(content)

        embd_lrn = self._build_embedding_entity(key, message, content, embedding)

        centroids = self._load_centroids()

        classified_email = self._classify_email(embedding, centroids, message)
        if classified_email is None:
            return

        embd_lrn.label = classified_email.label.value

        embd_lrn.meta = self._build_classification_meta(classified_email)

        embd_lrn_id = self._save_embd_lrn(embd_lrn)

        self._logger.info("EmbdLrn saved with id: %d", embd_lrn_id)

        ics_records = self._build_ics_records(embd_lrn_id, message.ics_files)
        if ics_records:
            self._save_ics_records(ics_records)

    def _parse_mail_message(self, value: dict[str, Any]) -> InboundMailMessage:
        subject = self._coerce_string(value.get("subject"))
        body = self._coerce_string(value.get("body"))
        sender_email = self._coerce_string(value.get("sender_email"))
        sender_name = self._coerce_string(value.get("sender_name"))

        recipient_name = self._coerce_optional_string(value.get("recipient_name"))
        recipient_email = self._coerce_optional_string(value.get("recipient_email"))
        content_type = self._coerce_optional_string(value.get("content_type"))

        ics_files = self._parse_ics_files(value.get("ics_files"))

        message = InboundMailMessage(
            subject=subject,
            body=body,
            sender_email=sender_email,
            sender_name=sender_name,
            recipient_name=recipient_name,
            recipient_email=recipient_email,
            content_type=content_type,
            ics_files=ics_files,
        )

        return message

    def _parse_ics_files(self, value: Any) -> list[InboundIcsFile]:
        if value is None:
            return []

        if not isinstance(value, list):
            self._logger.warning(
                "ICS payload has invalid type: %s",
                type(value).__name__,
            )

            return []

        parsed: list[InboundIcsFile] = []

        for idx, raw_item in enumerate(value):
            if not isinstance(raw_item, dict):
                self._logger.warning(
                    "ICS entry at index %d has invalid type: %s",
                    idx,
                    type(raw_item).__name__,
                )

                continue

            filename = self._coerce_string(raw_item.get("filename"))
            content_type = self._coerce_string(raw_item.get("content_type"))
            disposition = self._coerce_optional_string(raw_item.get("disposition"))
            method = self._coerce_optional_string(raw_item.get("method"))

            size_raw = raw_item.get("size")
            try:
                size = int(size_raw) if size_raw is not None else 0
            except (TypeError, ValueError):
                self._logger.warning(
                    "ICS entry at index %d has invalid size: %s",
                    idx,
                    size_raw,
                )

                size = 0

            content_raw = raw_item.get("content")
            if content_raw is None:
                content = None
            elif isinstance(content_raw, str):
                content = content_raw
            else:
                self._logger.warning(
                    "ICS entry at index %d has non-string content of type: %s",
                    idx,
                    type(content_raw).__name__,
                )

                content = None

            parsed.append(
                InboundIcsFile(
                    filename=filename,
                    content_type=content_type,
                    disposition=disposition,
                    method=method,
                    size=size,
                    content=content,
                )
            )

        return parsed

    def _coerce_string(self, value: Any) -> str:
        if isinstance(value, str):
            return value

        if value is None:
            return ""

        coerced = str(value)

        self._logger.warning(
            "Expected string value, received %s: coerced to string",
            type(value).__name__,
        )

        return coerced

    def _coerce_optional_string(self, value: Any) -> str | None:
        if value is None:
            return None

        if isinstance(value, str):
            return value

        coerced = str(value)

        self._logger.warning(
            "Expected optional string value, received %s: coerced to string",
            type(value).__name__,
        )

        return coerced

    def _build_content(self, subject: str, body: str) -> str:
        combined = f"SUBJECT {subject} BODY {body}"

        normalized = normalize_content(combined)

        return normalized

    def _build_embedding_entity(
        self,
        key: int,
        message: InboundMailMessage,
        content: str,
        embedding: list[float],
    ) -> EmbdLrn:
        embd_lrn = EmbdLrn(
            subject=message.subject,
            body=message.body,
            content=content,
            embedding=embedding,
            inbox_email_id=key,
            sender_email=message.sender_email,
            sender_name=message.sender_name,
            recipient_name=message.recipient_name,
            recipient_email=message.recipient_email,
            content_type=message.content_type,
        )

        return embd_lrn

    def _load_centroids(self) -> dict[str, list[ClusterCentroid]]:
        with self._db_factory() as db:
            rows = db.query(EmbdCntr).all()

        centroids: dict[str, list[ClusterCentroid]] = defaultdict(list)

        if not rows:
            message = "No EmbdCntr's found"
            self._logger.warning(message)

            return {}

        for row in rows:
            if row.embedding is None:
                message = f"No embedding in centroid with id {row.id}"
                self._logger.error(message)
                raise ValueError(message)

            cluster_id = row.cluster_id
            if cluster_id is None:
                self._logger.warning(
                    "Centroid id %s for label %s has no cluster_id; falling back to 0",
                    row.id,
                    row.label,
                )
                cluster_id = 0

            centroid = ClusterCentroid(
                label=row.label,
                cluster_id=cluster_id,
                embedding=row.embedding,
                threshold=row.threshold,
            )

            centroids[row.label].append(centroid)

        return {label: items for label, items in centroids.items()}

    def _prepare_service_centroids(
        self,
        centroids: dict[str, list[ClusterCentroid]],
    ) -> dict[str, list[tuple[int, list[float]]]]:
        payload: dict[str, list[tuple[int, list[float]]]] = {}

        for label, items in centroids.items():
            payload[label] = [
                (item.cluster_id, item.embedding)
                for item in items
            ]

        return payload

    def _locate_centroid(
        self,
        centroids: dict[str, list[ClusterCentroid]],
        label: str,
        cluster_id: int,
    ) -> ClusterCentroid | None:
        for candidate in centroids.get(label, []):
            if candidate.cluster_id == cluster_id:
                return candidate

        return None

    def _classify_email(
        self,
        embedding: list[float],
        centroids: dict[str, list[ClusterCentroid]],
        message: InboundMailMessage,
    ) -> ClassifiedEmail | None:
        if not centroids:
            self._logger.warning("Skipping classification: centroids cache empty")

            return None

        service_centroids = self._prepare_service_centroids(centroids)

        (
            label,
            cluster_id,
            score,
            top_two_delta,
            could_be,
            scores,
        ) = self._openai_service.classify_by_centroid(
            emb=embedding,
            centroids=service_centroids,
            threshold=None,
            margin=self._config.CLASSIFY_BY_CENTROID_MARGIN,
        )

        if label == "other" or cluster_id is None:
            self._logger.info(
                "Classification fell back to 'other'; could_be=%s", could_be
            )
            self._logger.info(
                f"Scores were (score = {score}): {scores}"
            )
            self._logger.info(
                f"top_two_delta were: {top_two_delta}"
            )
            self._logger.info(
                f"cluster_id were: {cluster_id}"
            )

            return None

        self._logger.info(
            "Email categorized as: %s (cluster %s), score: %f, top_two_delta: %s, could_be: %s",
            label,
            cluster_id,
            score,
            top_two_delta,
            could_be,
        )

        parsed_label = EmailLabel(label)

        if parsed_label == EmailLabel.MEETING_ACTION:
            parsed_label = self._derive_meeting_label(message.ics_files)

        centroid = self._locate_centroid(centroids, label, cluster_id)
        if centroid is None:
            self._logger.error(
                "Missing centroid definition for label %s and cluster %s", label, cluster_id
            )

            return None

        threshold_source = "cluster"
        threshold = centroid.threshold

        if threshold is None:
            thresholds_map = self._config.CLASSIFY_BY_CENTROID_THRESHOLDS
            label_key = parsed_label.value.lower()
            threshold = resolve_threshold(
                parsed_label.value,
                self._config.CLASSIFY_BY_CENTROID_THRESHOLDS,
                self._config.CLASSIFY_BY_CENTROID_THRESHOLD,
            )
            threshold_source = "class" if label_key in thresholds_map else "default"

        if score < threshold:
            self._logger.info(
                "Discarding classification %s (cluster %s): score %.5f below threshold %.5f",
                parsed_label.value,
                cluster_id,
                score,
                threshold,
            )

            return None

        classified_email = ClassifiedEmail(
            label=parsed_label,
            raw_label=label,
            cluster_id=cluster_id,
            score=score,
            threshold=threshold,
            threshold_source=threshold_source,
            top_two_delta=top_two_delta,
            could_be=could_be,
            scores=scores,
        )

        return classified_email

    def _build_classification_meta(
        self,
        classified_email: ClassifiedEmail,
    ) -> dict[str, Any]:
        def to_float(value: float | None) -> float | None:
            if value is None:
                return None

            return float(value)

        scores: list[dict[str, float | int | str]] = []
        for raw_label, cluster_scores in classified_email.scores.items():
            for cluster_id, score in cluster_scores.items():
                scores.append(
                    {
                        "label": raw_label,
                        "cluster_id": cluster_id,
                        "score": float(score),
                    }
                )

        scores.sort(key=lambda item: item["score"], reverse=True)

        meta = {
            "classification": {
                "raw_label": classified_email.raw_label,
                "final_label": classified_email.label.value,
                "cluster_id": classified_email.cluster_id,
                "score": float(classified_email.score),
                "threshold": float(classified_email.threshold),
                "threshold_source": classified_email.threshold_source,
                "top_two_delta": to_float(classified_email.top_two_delta),
                "could_be": classified_email.could_be,
                "scores": scores,
            }
        }

        return meta

    def _derive_meeting_label(self, ics_files: list[InboundIcsFile]) -> EmailLabel:
        for ics in ics_files:
            if self._is_cancellation_ics(ics):
                return EmailLabel.MEETING_CNCL

            method = self._get_ics_method(ics)
            if method != "REQUEST":
                continue

            return self._resolve_request_meeting_label(ics)

        return EmailLabel.MEETING_INV

    def _get_ics_method(self, ics: InboundIcsFile) -> str:
        method = ics.method or ""

        return method.upper()

    def _is_cancellation_ics(self, ics: InboundIcsFile) -> bool:
        method = self._get_ics_method(ics)
        if method == "CANCEL":
            return True

        if ics.content is None:
            return False

        content = ics.content.upper()
        if "METHOD:CANCEL" in content:
            return True

        for line in self._iterate_ics_lines(content):
            if line.startswith("STATUS:") and "CANCEL" in line:
                return True

            if line.startswith("SUMMARY") and "CANCEL" in line:
                return True

            if "CANCELLATION REASON" in line:
                return True

        return False

    def _resolve_request_meeting_label(self, ics: InboundIcsFile) -> EmailLabel:
        if ics.content is None:
            self._logger.warning(
                "ICS content missing for filename %s while deriving meeting label",
                ics.filename,
            )

            return EmailLabel.MEETING_UPD

        sequence = self._extract_ics_sequence(ics.content)
        if sequence == 0:
            return EmailLabel.MEETING_CRT

        return EmailLabel.MEETING_UPD

    def _extract_ics_sequence(self, content: str) -> int | None:
        for line in self._iterate_ics_lines(content):
            if not line.startswith("SEQUENCE:"):
                continue

            value = line.split("SEQUENCE:", maxsplit=1)[1]

            try:
                return int(value.strip())
            except ValueError:
                self._logger.warning("Failed to parse ICS SEQUENCE value: %s", value)

                return None

        return None

    def _iterate_ics_lines(self, content: str) -> Iterable[str]:
        normalized_content = content.replace("\r\n", "\n")

        for raw_line in normalized_content.split("\n"):
            yield raw_line.strip()

    def _save_embd_lrn(self, embd_lrn: EmbdLrn) -> int:
        with self._db_factory() as db:
            db.add(embd_lrn)
            db.commit()
            db.refresh(embd_lrn)

            embd_lrn_id = embd_lrn.id

        return embd_lrn_id

    def _build_ics_records(
        self,
        embd_lrn_id: int,
        ics_files: list[InboundIcsFile],
    ) -> list[IcsFileData]:
        records: list[IcsFileData] = []

        for ics in ics_files:
            record = IcsFileData(
                embd_lrn_id=embd_lrn_id,
                filename=ics.filename,
                content_type=ics.content_type,
                disposition=ics.disposition,
                method=ics.method,
                size=ics.size,
                content=ics.content or "",
            )

            records.append(record)

        return records

    def _save_ics_records(self, records: list[IcsFileData]) -> None:
        with self._db_factory() as db:
            db.add_all(records)
            db.commit()


def get_kafka_event_handler(
    config: Config | None = None,
    openai_service: OpenAIService | None = None,
    db_factory: DbSessionFactory | None = None,
) -> KafkaNewMailHandler:
    config_to_use = config or Config()
    service = openai_service or get_openai_service()
    factory = db_factory or get_db_context

    handler = KafkaNewMailHandler(
        config=config_to_use,
        openai_service=service,
        db_factory=factory,
    )

    return handler
