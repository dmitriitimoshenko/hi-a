from __future__ import annotations

import logging
from typing import Iterable

from prometheus_client import CollectorRegistry, Gauge
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.models import (
    MailMappingFeedback,
    MailMappingMatch,
    MailMappingMatchCandidate,
)


logger = logging.getLogger(__name__)


class MetricsCollector:
    def __init__(self, session: Session) -> None:
        self._session = session

    def collect(self) -> CollectorRegistry:
        registry = CollectorRegistry()

        try:
            self._collect_match_metrics(registry)
            self._collect_feedback_metrics(registry)
        except Exception:  # pragma: no cover
            logger.exception("Failed to collect metrics")

        return registry

    def _collect_match_metrics(self, registry: CollectorRegistry) -> None:
        matches_total = Gauge(
            "mail_mapper_matches_total",
            "Total number of match attempts",
            registry=registry,
        )
        matches_total.set(self._count(MailMappingMatch))

        match_status = Gauge(
            "mail_mapper_matches_by_status_total",
            "Matches grouped by assignment status",
            labelnames=["status"],
            registry=registry,
        )
        match_status.labels(status="assigned").set(
            self._count(MailMappingMatch, MailMappingMatch.application_id.is_not(None))
        )
        match_status.labels(status="skipped").set(
            self._count(MailMappingMatch, MailMappingMatch.application_id.is_(None))
        )

        avg_overall = Gauge(
            "mail_mapper_match_overall_score_average",
            "Average overall score for persisted matches",
            registry=registry,
        )
        avg_overall.set(self._avg(MailMappingMatch.overall_score))

        avg_raw_overall = Gauge(
            "mail_mapper_match_raw_score_average",
            "Average raw (pre-calibration) score",
            registry=registry,
        )
        avg_raw_overall.set(self._avg(MailMappingMatch.raw_overall_score))

        config_distribution = Gauge(
            "mail_mapper_matches_by_config_total",
            "Matches grouped by scoring config version",
            labelnames=["version"],
            registry=registry,
        )
        for version, count in self._grouped_counts(
            MailMappingMatch.scoring_config_version
        ):
            config_distribution.labels(version=version).set(count)

        candidate_avg_score = Gauge(
            "mail_mapper_candidate_score_average",
            "Average candidate aggregated score",
            registry=registry,
        )
        candidate_avg_score.set(self._avg(MailMappingMatchCandidate.aggregated_score))

        candidate_avg_raw = Gauge(
            "mail_mapper_candidate_raw_score_average",
            "Average candidate raw aggregated score",
            registry=registry,
        )
        candidate_avg_raw.set(
            self._avg(MailMappingMatchCandidate.raw_aggregated_score)
        )

    def _collect_feedback_metrics(self, registry: CollectorRegistry) -> None:
        feedback_total = Gauge(
            "mail_mapper_feedback_total",
            "Feedback records grouped by reason",
            labelnames=["reason"],
            registry=registry,
        )
        for reason, count in self._grouped_counts(MailMappingFeedback.reason):
            feedback_total.labels(reason=reason).set(count)

    def _count(self, model: type, *filters: object) -> float:
        stmt = select(func.count()).select_from(model)
        for condition in filters:
            stmt = stmt.where(condition)

        result = self._session.execute(stmt).scalar_one()

        return float(result or 0)

    def _avg(self, column: object) -> float:
        stmt = select(func.avg(column))
        value = self._session.execute(stmt).scalar_one()

        return float(value or 0.0)

    def _grouped_counts(self, column: object) -> Iterable[tuple[str, float]]:
        stmt = select(column, func.count()).group_by(column)
        rows = self._session.execute(stmt).all()

        for label, count in rows:
            resolved_label = str(label or "unknown")
            yield resolved_label, float(count or 0)
