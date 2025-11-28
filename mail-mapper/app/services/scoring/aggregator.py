from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from app.enums import ScoreComponentType
from app.services.scoring.config import ScoringConfig, ScoringConfigProvider
from app.services.scoring.features import FeatureValue


@dataclass
class AggregatedComponent:
    component: ScoreComponentType
    value: float
    weight: float
    available: bool


@dataclass
class AggregationResult:
    score: float
    raw_score: float
    config: ScoringConfig
    components: list[AggregatedComponent]


class ScoreAggregator:
    def __init__(self, config_provider: ScoringConfigProvider) -> None:
        self._config_provider = config_provider

    def aggregate(self, features: Iterable[FeatureValue]) -> AggregationResult:
        config = self._config_provider.get_config()
        weights = config.weights

        entries: list[AggregatedComponent] = []
        raw_value = 0.0

        for feature in features:
            weight = weights.resolve(feature.component)
            entry = AggregatedComponent(
                component=feature.component,
                value=feature.value,
                weight=weight,
                available=feature.available,
            )
            entries.append(entry)

            raw_value += feature.value * weight

        calibrated = raw_value
        if config.calibration is not None:
            calibrated = config.calibration.apply(raw_value)

        return AggregationResult(
            score=calibrated,
            raw_score=raw_value,
            config=config,
            components=entries,
        )

