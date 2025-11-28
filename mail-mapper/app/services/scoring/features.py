from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime
from math import exp
from typing import Protocol

from app.enums import EmailLabel, ScoreComponentType
from app.integrations.open_ai.service.service import OpenAIService

logger = logging.getLogger(__name__)


@dataclass
class FeatureValue:
    component: ScoreComponentType
    value: float
    available: bool


class FeatureCalculator(Protocol):
    component: ScoreComponentType

    def calculate(self, context: "FeatureContext") -> FeatureValue:
        ...


@dataclass
class FeatureContext:
    email_label: EmailLabel
    email_created_at: datetime
    email_sender_email: str
    email_sender_name: str
    email_embedding: list[float] | None
    application: dict[str, object]


class SimilarityFeatureCalculator:
    component = ScoreComponentType.SIMILARITY

    def __init__(self, openai_service: OpenAIService) -> None:
        self._openai_service = openai_service

    def calculate(self, context: FeatureContext) -> FeatureValue:
        email_embedding = context.email_embedding
        if not email_embedding:
            return FeatureValue(
                component=self.component,
                value=0.0,
                available=False,
            )

        application_embedding = context.application.get("embedding")
        if not isinstance(application_embedding, list):
            return FeatureValue(
                component=self.component,
                value=0.0,
                available=False,
            )

        similarity = self._openai_service.compare_vectors(
            email_embedding,
            application_embedding,
        )

        scaled = (similarity + 1.0) / 2.0

        return FeatureValue(
            component=self.component,
            value=scaled,
            available=True,
        )


class TimeGapFeatureCalculator:
    component = ScoreComponentType.TIME

    def __init__(self, tau_resolver: "TimeTauResolver") -> None:
        self._tau_resolver = tau_resolver

    def calculate(self, context: FeatureContext) -> FeatureValue:
        raw_created_at = context.application.get("created_at")
        if not isinstance(raw_created_at, str):
            return FeatureValue(
                component=self.component,
                value=0.0,
                available=False,
            )

        try:
            application_created_at = datetime.fromisoformat(
                raw_created_at.replace("Z", "+00:00")
            )
        except ValueError:
            logger.debug("Failed to parse application created_at: %s", raw_created_at)

            return FeatureValue(
                component=self.component,
                value=0.0,
                available=False,
            )

        tau = self._tau_resolver.resolve(context.email_label, context.application)

        gap_minutes = abs(
            context.email_created_at.timestamp()
            - application_created_at.timestamp()
        ) / 60.0

        score = float(max(min(exp(-gap_minutes / tau), 1.0), 0.0))

        return FeatureValue(
            component=self.component,
            value=score,
            available=True,
        )


class SenderFeatureCalculator:
    component = ScoreComponentType.SENDER

    def calculate(self, context: FeatureContext) -> FeatureValue:
        try:
            company = context.application["company"]
            contacts = context.application.get("meta", {}).get("contacts")
        except Exception as e:
            logger.error("Failed to calculate sender feature: %s", e)

            return FeatureValue(
                component=self.component,
                value=0.0,
                available=False,
            )

        sender_tokens = self._collect_sender_tokens(
            context.email_sender_email,
            context.email_sender_name,
        )
        application_tokens = self._collect_application_tokens(
            company,
            contacts,
        )

        matches = sum(1 for token in sender_tokens if token in application_tokens)
        score = 1.0 - exp(-matches)

        return FeatureValue(
            component=self.component,
            value=score,
            available=matches > 0,
        )

    def _collect_sender_tokens(
        self,
        email_sender_email: str,
        email_sender_name: str,
    ) -> list[str]:
        email_tokens = self._to_tokens(email_sender_email)
        if email_tokens:
            email_tokens = email_tokens[:-1]

        name_tokens = self._to_tokens(email_sender_name)

        tokens: list[str] = []
        tokens.extend(email_tokens)
        tokens.extend(name_tokens)

        return tokens

    def _collect_application_tokens(
        self,
        app_company: str,
        app_contacts: str | None,
    ) -> list[str]:
        company_tokens = self._to_tokens(app_company)
        contact_tokens = self._to_tokens(app_contacts or "")

        tokens: list[str] = []
        tokens.extend(company_tokens)
        tokens.extend(contact_tokens)

        return tokens

    def _to_tokens(self, text: str) -> list[str]:
        if not text:
            return []

        lowered = text.lower()
        stripped = lowered.replace("@", " ")

        tokens = re.findall(r"[a-z0-9]+", stripped)

        return tokens


class TimeTauResolver(Protocol):
    def resolve(
        self,
        label: EmailLabel,
        application: dict[str, object],
    ) -> float:
        ...

