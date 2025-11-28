from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from math import exp
from typing import Protocol

from app.enums import ScoreComponentType


DEFAULT_SCORE_WEIGHTS: dict[ScoreComponentType, float] = {
    ScoreComponentType.SIMILARITY: 0.44,
    ScoreComponentType.TIME: 0.2,
    ScoreComponentType.SENDER: 0.36,
}


@dataclass
class ScoreWeights:
    values: dict[ScoreComponentType, float]
    version: str
    updated_at: datetime

    def resolve(self, component: ScoreComponentType) -> float:
        weight = self.values.get(component, 0.0)

        return weight


@dataclass
class ScoreCalibration:
    bias: float
    scale: float
    updated_at: datetime
    version: str

    def apply(self, score: float) -> float:
        z = self.bias + self.scale * score
        calibrated = 1.0 / (1.0 + exp(-z))

        return calibrated


@dataclass
class ScoringConfig:
    weights: ScoreWeights
    calibration: ScoreCalibration | None

    @classmethod
    def default(cls) -> "ScoringConfig":
        now = datetime.utcnow()

        weights = ScoreWeights(
            values=DEFAULT_SCORE_WEIGHTS.copy(),
            version="static-default",
            updated_at=now,
        )

        return cls(weights=weights, calibration=None)


class ScoringConfigProvider(Protocol):
    def get_config(self) -> ScoringConfig:
        ...
