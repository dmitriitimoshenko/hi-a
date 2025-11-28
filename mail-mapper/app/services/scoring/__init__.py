"""Scoring utilities for mail-mapper."""

from app.services.scoring.aggregator import ScoreAggregator
from app.services.scoring.config import ScoringConfig

__all__ = [
    "ScoreAggregator",
    "ScoringConfig",
]
