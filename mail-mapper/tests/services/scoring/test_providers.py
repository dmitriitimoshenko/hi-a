from __future__ import annotations

import os

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

os.environ.setdefault("DATABASE_MAIL_MAPPER_URL", "sqlite+pysqlite:///:memory:")

from app.database import Base  # noqa: E402  pylint: disable=wrong-import-position
from app.enums import ScoreComponentType  # noqa: E402  pylint: disable=wrong-import-position
from app.models import (  # noqa: E402  pylint: disable=wrong-import-position
    ScoringCalibration,
    ScoringWeight,
    ScoringWeightEntry,
)
from app.services.scoring.providers import (  # noqa: E402  pylint: disable=wrong-import-position
    DatabaseScoringConfigProvider,
)


def _create_session() -> Session:
    engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    factory = sessionmaker(bind=engine, future=True)

    return factory()


def test_database_provider_returns_defaults_when_empty() -> None:
    session = _create_session()
    provider = DatabaseScoringConfigProvider(session)

    config = provider.get_config()

    assert config.weights.values[ScoreComponentType.SIMILARITY] == 0.44
    assert config.weights.values[ScoreComponentType.TIME] == 0.2
    assert config.weights.values[ScoreComponentType.SENDER] == 0.36
    assert config.calibration is None

    session.close()


def test_database_provider_loads_active_weights_and_calibration() -> None:
    session = _create_session()

    with session.begin():
        weight = ScoringWeight(
            version="experiment-v1",
            description="test configuration",
            is_active=True,
        )
        session.add(weight)
        session.flush()

        entries = [
            ScoringWeightEntry(
                weight_id=weight.id,
                component=ScoreComponentType.SIMILARITY.value,
                value=0.5,
            ),
            ScoringWeightEntry(
                weight_id=weight.id,
                component=ScoreComponentType.TIME.value,
                value=0.3,
            ),
            ScoringWeightEntry(
                weight_id=weight.id,
                component=ScoreComponentType.SENDER.value,
                value=0.2,
            ),
        ]
        session.add_all(entries)

        calibration = ScoringCalibration(
            version="calibration-v1",
            description="test calibration",
            bias=-0.5,
            scale=1.5,
            is_active=True,
        )
        session.add(calibration)

    provider = DatabaseScoringConfigProvider(session)
    config = provider.get_config()

    assert config.weights.version == "experiment-v1"
    assert config.weights.values[ScoreComponentType.SIMILARITY] == 0.5
    assert config.weights.values[ScoreComponentType.TIME] == 0.3
    assert config.weights.values[ScoreComponentType.SENDER] == 0.2
    assert config.calibration is not None
    assert config.calibration.version == "calibration-v1"
    assert config.calibration.bias == -0.5
    assert config.calibration.scale == 1.5

    session.close()
