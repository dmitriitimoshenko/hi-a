from __future__ import annotations

import threading
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, selectinload

from app.enums import ScoreComponentType
from app.models import ScoringCalibration, ScoringWeight
from app.services.scoring.config import (
    DEFAULT_SCORE_WEIGHTS,
    ScoreCalibration,
    ScoreWeights,
    ScoringConfig,
    ScoringConfigProvider,
)


class StaticScoringConfigProvider(ScoringConfigProvider):
    def __init__(self) -> None:
        self._config = ScoringConfig.default()

    def get_config(self) -> ScoringConfig:
        return self._config


class DatabaseScoringConfigProvider(ScoringConfigProvider):
    def __init__(self, session: Session) -> None:
        self._session = session
        self._lock = threading.Lock()
        self._cached_config: ScoringConfig | None = None
        self._cached_at: datetime | None = None

    def get_config(self) -> ScoringConfig:
        with self._lock:
            if self._cached_config is None:
                self._cached_config = self._load_config()
                self._cached_at = datetime.utcnow()

            return self._cached_config

    def refresh(self) -> None:
        with self._lock:
            self._cached_config = self._load_config()
            self._cached_at = datetime.utcnow()

    def _load_config(self) -> ScoringConfig:
        try:
            weight_model = self._fetch_active_weight()
            calibration_model = self._fetch_active_calibration()
        except SQLAlchemyError:
            return ScoringConfig.default()

        if weight_model is None:
            weights = ScoreWeights(
                values=DEFAULT_SCORE_WEIGHTS.copy(),
                version="db-default",
                updated_at=datetime.utcnow(),
            )
        else:
            values = self._resolve_weight_values(weight_model)
            weights = ScoreWeights(
                values=values,
                version=weight_model.version,
                updated_at=weight_model.updated_at,
            )

        calibration = self._resolve_calibration(calibration_model)

        return ScoringConfig(weights=weights, calibration=calibration)

    def _fetch_active_weight(self) -> ScoringWeight | None:
        stmt = (
            select(ScoringWeight)
            .options(selectinload(ScoringWeight.entries))
            .where(ScoringWeight.is_active.is_(True))
            .order_by(ScoringWeight.updated_at.desc())
            .limit(1)
        )

        result = self._session.execute(stmt).scalars().first()

        return result

    def _fetch_active_calibration(self) -> ScoringCalibration | None:
        stmt = (
            select(ScoringCalibration)
            .where(ScoringCalibration.is_active.is_(True))
            .order_by(ScoringCalibration.updated_at.desc())
            .limit(1)
        )

        return self._session.execute(stmt).scalars().first()

    def _resolve_weight_values(
        self,
        weight: ScoringWeight,
    ) -> dict[ScoreComponentType, float]:
        values = DEFAULT_SCORE_WEIGHTS.copy()

        for entry in weight.entries:
            try:
                component = ScoreComponentType(entry.component)
            except ValueError:
                continue

            values[component] = entry.value

        return values

    def _resolve_calibration(
        self,
        calibration: ScoringCalibration | None,
    ) -> ScoreCalibration | None:
        if calibration is None:
            return None

        return ScoreCalibration(
            bias=calibration.bias,
            scale=calibration.scale,
            updated_at=calibration.updated_at,
            version=calibration.version,
        )
