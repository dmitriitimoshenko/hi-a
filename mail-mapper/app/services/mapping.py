from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

import numpy as np
from fastapi import Depends
from sqlalchemy.orm import Session

from app.config import Config
from app.database import get_db
from app.enums import ApplicationStatus, EmailLabel, FeedbackReason, ScoreComponentType
from app.integrations.open_ai.service.service import (
    OpenAIService,
    provide_openai_service,
)
from app.models import (
    MailMappingFeedback,
    MailMappingMatch as MailMappingMatchModel,
    MailMappingMatchCandidate,
    MailMappingMatchCandidateScore,
)
from app.services.scoring.aggregator import ScoreAggregator
from app.services.scoring.config import ScoringConfigProvider
from app.services.scoring.features import (
    FeatureContext,
    SenderFeatureCalculator,
    SimilarityFeatureCalculator,
    TimeGapFeatureCalculator,
    TimeTauResolver,
)
from app.services.scoring.providers import (
    DatabaseScoringConfigProvider,
    StaticScoringConfigProvider,
)
from app.services.scoring.time_resolver import StaticTimeTauResolver
from app.storage_client.google_sheets_accessor_client import (
    GoogleSheetsAccessorClient,
    get_google_sheets_accessor_client,
)
from app.tools.hungarian import linear_sum_assignment


logger = logging.getLogger(__name__)


@dataclass
class EmailRecord:
    id: int
    label: EmailLabel
    created_at: datetime
    sender_email: str
    sender_name: str
    embedding: list[float]


@dataclass
class ResolvedMatch:
    email_id: int
    application: dict[str, Any]
    score: float


@dataclass
class MailMappingResult:
    matches: list[ResolvedMatch]
    skipped_email_ids: list[int]


@dataclass
class CandidateScoreEntry:
    score_type: ScoreComponentType
    score: float
    weight: float
    available: bool | None = None


@dataclass
class CandidateSnapshot:
    application_id: int
    aggregated_score: float
    raw_aggregated_score: float
    score_entries: list[CandidateScoreEntry]
    scoring_config_version: str
    calibration_version: str | None


@dataclass
class MatchPersistencePayload:
    email_id: int
    email_label: EmailLabel
    application_id: int | None
    overall_score: float | None
    raw_overall_score: float | None
    scoring_config_version: str | None
    calibration_version: str | None
    min_match_score: float
    matching_batch_id: UUID
    candidate_snapshots: list[CandidateSnapshot]


@dataclass
class MatchAssignment:
    application: dict[str, Any]
    application_id: int | None
    score: float


@dataclass
class MatchBatchResult:
    assignments: dict[int, MatchAssignment]
    candidate_snapshots: dict[int, list[CandidateSnapshot]]


class EmailApplicationMatcher:
    def __init__(
        self,
        openai_service: OpenAIService,
        *,
        min_match_score: float = 0.5,
        config_provider: ScoringConfigProvider | None = None,
        tau_resolver: TimeTauResolver | None = None,
    ) -> None:
        resolved_provider = config_provider or StaticScoringConfigProvider()
        resolved_tau_resolver = tau_resolver or StaticTimeTauResolver()

        self._similarity_feature = SimilarityFeatureCalculator(
            openai_service=openai_service,
        )
        self._time_gap_feature = TimeGapFeatureCalculator(
            tau_resolver=resolved_tau_resolver,
        )
        self._sender_feature = SenderFeatureCalculator()
        self._aggregator = ScoreAggregator(resolved_provider)
        self._logger = logging.getLogger(__name__)
        self._min_match_score = min_match_score

    def match_batch(
        self,
        emails: list[EmailRecord],
        applications: list[dict[str, Any]],
        *,
        blocked_application_ids: dict[int, set[int]] | None = None,
    ) -> MatchBatchResult:
        if not emails or not applications:
            return MatchBatchResult(assignments={}, candidate_snapshots={})

        resolved_blocked: dict[int, set[int]] = blocked_application_ids or {}

        self._ensure_embeddings_present(emails)

        score_matrix, candidate_snapshots_map = self._build_score_matrix(
            emails,
            applications,
            resolved_blocked,
        )

        num_emails, num_applications = score_matrix.shape
        max_size = max(num_emails, num_applications)

        padded_cost = np.ones((max_size, max_size), dtype=float)
        padded_cost[:num_emails, :num_applications] = -np.log(score_matrix + 1e-12)

        row_indices, column_indices = linear_sum_assignment(padded_cost)

        matches: dict[int, MatchAssignment] = {}

        row_sequence: list[int] = [int(value) for value in row_indices]
        column_sequence: list[int] = [int(value) for value in column_indices]

        for row, col in zip(row_sequence, column_sequence, strict=False):
            if row >= num_emails or col >= num_applications:
                continue

            score = float(score_matrix[row, col])
            email = emails[row]
            application = applications[col]
            application_id = self._extract_application_id(application)

            self._logger.info(
                "Mapping email %d to application %s with score %.4f (threshold %.4f)",
                email.id,
                application_id,
                score,
                self._min_match_score,
            )
            if score < self._min_match_score:
                continue

            matches[email.id] = MatchAssignment(
                application=application,
                application_id=application_id,
                score=score,
            )

        self._log_assignments(emails, matches)

        result = MatchBatchResult(
            assignments=matches,
            candidate_snapshots=candidate_snapshots_map,
        )

        return result

    def _extract_application_id(self, application: dict[str, Any]) -> int | None:
        try:
            application_id = int(application["id"])
        except (KeyError, TypeError, ValueError):
            return None

        return application_id

    def _ensure_embeddings_present(self, emails: list[EmailRecord]) -> None:
        missing = [email.id for email in emails if not email.embedding]
        if not missing:
            return

        self._logger.warning(
            "Emails missing embeddings: %s; similarity feature fallback will be used",
            missing,
        )

    def _build_score_matrix(
        self,
        emails: list[EmailRecord],
        applications: list[dict[str, Any]],
        blocked_application_ids: dict[int, set[int]],
    ) -> tuple[np.ndarray, dict[int, list[CandidateSnapshot]]]:
        rows: list[list[float]] = []
        candidate_snapshots: dict[int, list[CandidateSnapshot]] = {}

        for email in emails:
            email_blocked_ids = blocked_application_ids.get(email.id) or set()
            row, snapshots = self._build_score_row(
                email,
                applications,
                email_blocked_ids,
            )
            rows.append(row)
            candidate_snapshots[email.id] = snapshots
            self._log_candidate_snapshots(email, snapshots)

        matrix = np.array(rows, dtype=float)

        return matrix, candidate_snapshots

    def _build_score_row(
        self,
        email: EmailRecord,
        applications: list[dict[str, Any]],
        blocked_application_ids: set[int],
    ) -> tuple[list[float], list[CandidateSnapshot]]:
        row: list[float] = []
        candidate_snapshots: list[CandidateSnapshot] = []

        for application in applications:
            application_id = self._extract_application_id(application)
            if application_id is None:
                row.append(0.0)
                continue

            is_blocked = application_id in blocked_application_ids

            context = FeatureContext(
                email_label=email.label,
                email_created_at=email.created_at,
                email_sender_email=email.sender_email,
                email_sender_name=email.sender_name,
                email_embedding=email.embedding,
                application=application,
            )

            feature_values = [
                self._similarity_feature.calculate(context),
                self._time_gap_feature.calculate(context),
                self._sender_feature.calculate(context),
            ]

            aggregation = self._aggregator.aggregate(feature_values)

            score_value = aggregation.score
            if is_blocked:
                self._logger.info(
                    "Email %d application %s suppressed due to feedback skip",
                    email.id,
                    application_id,
                )
                score_value = 0.0

            row.append(score_value)

            score_entries = [
                CandidateScoreEntry(
                    score_type=component.component,
                    score=component.value,
                    weight=component.weight,
                    available=component.available,
                )
                for component in aggregation.components
            ]

            candidate_snapshots.append(
                CandidateSnapshot(
                    application_id=application_id,
                    aggregated_score=aggregation.score,
                    raw_aggregated_score=aggregation.raw_score,
                    score_entries=score_entries,
                    scoring_config_version=aggregation.config.weights.version,
                    calibration_version=(
                        aggregation.config.calibration.version
                        if aggregation.config.calibration
                        else None
                    ),
                )
            )

        if not row:
            row.append(0.0)

        return row, candidate_snapshots

    def _log_candidate_snapshots(
        self,
        email: EmailRecord,
        snapshots: list[CandidateSnapshot],
    ) -> None:
        if not snapshots:
            self._logger.info("Email %d has no candidate snapshots", email.id)

            return

        payload = [
            (
                snapshot.application_id,
                snapshot.aggregated_score,
                snapshot.raw_aggregated_score,
                snapshot.scoring_config_version,
                snapshot.calibration_version,
                [
                    (entry.score_type.value, entry.score, entry.weight, entry.available)
                    for entry in snapshot.score_entries
                ],
            )
            for snapshot in snapshots
        ]

        self._logger.info("Email %d scoring payload: %s", email.id, payload)

    def _log_assignments(
        self,
        emails: list[EmailRecord],
        matches: dict[int, MatchAssignment],
    ) -> None:
        matched_payload: list[tuple[int, int | None, float]] = []
        unmatched_emails: list[int] = []

        for email in emails:
            match = matches.get(email.id)
            if match is None:
                unmatched_emails.append(email.id)
                continue

            matched_payload.append(
                (email.id, match.application_id, match.score)
            )

        if matched_payload:
            self._logger.info("Hungarian assignments: %s", matched_payload)

        if unmatched_emails:
            self._logger.info("Emails without assignment: %s", unmatched_emails)


class MailMappingService:
    def __init__(
        self,
        storage_client: GoogleSheetsAccessorClient,
        matcher: EmailApplicationMatcher,
        db: Session,
        *,
        min_match_score: float,
    ) -> None:
        self._storage_client = storage_client
        self._matcher = matcher
        self._db = db
        self._logger = logging.getLogger(__name__)
        self._min_match_score = min_match_score

    def resolve_mappings(
        self,
        status: EmailLabel,
        emails: list[EmailRecord],
    ) -> MailMappingResult:
        status_include, status_exclude = self._resolve_application_filters(status)

        try:
            applications = self._storage_client.list_applications_without_reply(
                status_include=status_include,
                status_exclude=status_exclude,
            )
        except Exception as e:
            message = f"Failed to get applications with no reply: {e}"
            self._logger.error(message)
            raise ValueError(message)

        if not applications:
            skipped_ids = [email.id for email in emails]
            self._logger.info(
                "No applications available. Skipping emails %s",
                skipped_ids,
            )

            result = MailMappingResult(matches=[], skipped_email_ids=skipped_ids)

            return result

        email_ids = [email.id for email in emails]
        blocked_application_ids = self._load_blocked_application_ids(
            email_ids=email_ids,
        )

        batch_result = self._matcher.match_batch(
            emails,
            applications,
            blocked_application_ids=blocked_application_ids,
        )

        match_payloads: list[ResolvedMatch] = []
        skipped_ids: list[int] = []

        persistence_payloads: list[MatchPersistencePayload] = []
        matching_batch_id = uuid4()

        for email in emails:
            assignment = batch_result.assignments.get(email.id)
            snapshots = batch_result.candidate_snapshots.get(email.id, [])

            if assignment is None:
                skipped_ids.append(email.id)
            else:
                match_payloads.append(
                    ResolvedMatch(
                        email_id=email.id,
                        application=assignment.application,
                        score=assignment.score,
                    )
                )

            payload = self._build_persistence_payload(
                email=email,
                assignment=assignment,
                candidate_snapshots=snapshots,
                matching_batch_id=matching_batch_id,
            )
            persistence_payloads.append(payload)

        self._persist_matches(persistence_payloads)

        result = MailMappingResult(
            matches=match_payloads,
            skipped_email_ids=skipped_ids,
        )

        return result

    def _build_persistence_payload(
        self,
        *,
        email: EmailRecord,
        assignment: MatchAssignment | None,
        candidate_snapshots: list[CandidateSnapshot],
        matching_batch_id: UUID,
    ) -> MatchPersistencePayload:
        snapshot = self._select_snapshot(
            assignment=assignment,
            candidate_snapshots=candidate_snapshots,
        )

        if assignment is not None:
            overall_score = assignment.score
        elif snapshot is not None:
            overall_score = snapshot.aggregated_score
        else:
            overall_score = None

        raw_overall_score = snapshot.raw_aggregated_score if snapshot else None
        scoring_config_version = (
            snapshot.scoring_config_version if snapshot else None
        )
        calibration_version = snapshot.calibration_version if snapshot else None

        payload = MatchPersistencePayload(
            email_id=email.id,
            email_label=email.label,
            application_id=assignment.application_id if assignment else None,
            overall_score=overall_score,
            raw_overall_score=raw_overall_score,
            scoring_config_version=scoring_config_version,
            calibration_version=calibration_version,
            min_match_score=self._min_match_score,
            matching_batch_id=matching_batch_id,
            candidate_snapshots=candidate_snapshots,
        )

        return payload

    def _select_snapshot(
        self,
        *,
        assignment: MatchAssignment | None,
        candidate_snapshots: list[CandidateSnapshot],
    ) -> CandidateSnapshot | None:
        if not candidate_snapshots:
            return None

        if assignment and assignment.application_id is not None:
            for snapshot in candidate_snapshots:
                if snapshot.application_id == assignment.application_id:
                    return snapshot

        best_snapshot = max(
            candidate_snapshots,
            key=lambda item: item.aggregated_score,
            default=None,
        )

        return best_snapshot

    def _persist_matches(
        self,
        payloads: list[MatchPersistencePayload],
    ) -> None:
        if not payloads:
            return

        try:
            for payload in payloads:
                self._create_match_row(payload)
            self._db.commit()
        except Exception as e:
            self._db.rollback()
            message = f"Failed to persist match results: {e}"
            self._logger.error(message)
            raise ValueError(message) from e

    def _create_match_row(
        self,
        payload: MatchPersistencePayload,
    ) -> MailMappingMatchModel:
        match_row = MailMappingMatchModel(
            matching_batch_id=payload.matching_batch_id,
            email_id=payload.email_id,
            application_id=payload.application_id,
            email_label=payload.email_label.value,
            min_match_score=payload.min_match_score,
            overall_score=payload.overall_score,
            raw_overall_score=payload.raw_overall_score,
            scoring_config_version=payload.scoring_config_version,
            calibration_version=payload.calibration_version,
        )

        self._db.add(match_row)
        self._db.flush()

        self._create_candidate_rows(
            match_row=match_row,
            candidate_snapshots=payload.candidate_snapshots,
        )

        return match_row

    def _create_candidate_rows(
        self,
        *,
        match_row: MailMappingMatchModel,
        candidate_snapshots: list[CandidateSnapshot],
    ) -> None:
        sorted_candidates = sorted(
            candidate_snapshots,
            key=lambda item: item.aggregated_score,
            reverse=True,
        )

        for rank, snapshot in enumerate(sorted_candidates):
            candidate_row = MailMappingMatchCandidate(
                match_id=match_row.id,
                application_id=snapshot.application_id,
                aggregated_score=snapshot.aggregated_score,
                raw_aggregated_score=snapshot.raw_aggregated_score,
                scoring_config_version=snapshot.scoring_config_version,
                calibration_version=snapshot.calibration_version,
                rank=rank,
                is_selected=bool(
                    match_row.application_id is not None
                    and snapshot.application_id == match_row.application_id
                ),
            )

            self._db.add(candidate_row)
            self._db.flush()

            self._create_candidate_score_rows(
                candidate_id=candidate_row.id,
                score_entries=snapshot.score_entries,
            )

    def _create_candidate_score_rows(
        self,
        *,
        candidate_id: int,
        score_entries: list[CandidateScoreEntry],
    ) -> None:
        for entry in score_entries:
            score_row = MailMappingMatchCandidateScore(
                candidate_id=candidate_id,
                score_type=entry.score_type.value,
                score=entry.score,
                weight=entry.weight,
            )

            self._db.add(score_row)

    def _load_blocked_application_ids(
        self,
        *,
        email_ids: list[int],
    ) -> dict[int, set[int]]:
        if not email_ids:
            return {}

        unique_email_ids = list({int(value) for value in email_ids})
        if not unique_email_ids:
            return {}

        skip_reason_values = [reason.value for reason in FeedbackReason]

        query = (
            self._db.query(
                MailMappingFeedback.email_id,
                MailMappingFeedback.application_id,
            )
            .where(MailMappingFeedback.email_id.in_(unique_email_ids))
            .where(MailMappingFeedback.reason.in_(skip_reason_values))
            .where(MailMappingFeedback.application_id.is_not(None))
        )

        rows = query.all()

        blocked: dict[int, set[int]] = {}
        for email_id, application_id in rows:
            if application_id is None:
                continue

            resolved_email_id = int(email_id)
            resolved_application_id = int(application_id)

            blocked.setdefault(resolved_email_id, set()).add(resolved_application_id)

        if blocked:
            self._logger.info("Blocked mappings from feedback: %s", blocked)

        return blocked

    def _resolve_application_filters(
        self,
        status: EmailLabel,
    ) -> tuple[list[ApplicationStatus] | None, list[ApplicationStatus] | None]:
        if status == EmailLabel.APPLIED:
            return [ApplicationStatus.APPLIED], None

        if status == EmailLabel.DENIED:
            return None, [ApplicationStatus.DENIED]

        if status == EmailLabel.MEETING_INV:
            return None, [
                ApplicationStatus.MEETING,
                ApplicationStatus.DENIED,
                ApplicationStatus.OFFER,
            ]

        if status in (
            EmailLabel.MEETING_CRT,
            EmailLabel.MEETING_UPD,
            EmailLabel.MEETING_CNCL,
        ):
            return [
                ApplicationStatus.MEETING,
                ApplicationStatus.APPLIED,
                ApplicationStatus.PENDING,
            ], None

        if status == EmailLabel.OFFER:
            return [ApplicationStatus.OFFER], None

        message = f"Unsupported label for _resolve_application_filters: {status}"
        raise ValueError(message)


def get_mail_mapping_service(
    storage_client: GoogleSheetsAccessorClient = Depends(
        get_google_sheets_accessor_client
    ),
    openai_service: OpenAIService = Depends(provide_openai_service),
    db_session: Session = Depends(get_db),
) -> MailMappingService:
    config = Config()
    config_provider = DatabaseScoringConfigProvider(db_session)
    matcher = EmailApplicationMatcher(
        openai_service=openai_service,
        min_match_score=config.HUNGARIAN_MIN_MATCH_SCORE,
        config_provider=config_provider,
    )

    service = MailMappingService(
        storage_client=storage_client,
        matcher=matcher,
        db=db_session,
        min_match_score=config.HUNGARIAN_MIN_MATCH_SCORE,
    )

    return service
