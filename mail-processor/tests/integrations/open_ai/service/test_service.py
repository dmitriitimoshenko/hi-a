from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.integrations.open_ai.service.service import OpenAIService


def _build_service() -> OpenAIService:
    client = MagicMock()

    return OpenAIService(client)


def test_classify_by_centroid_ignores_margin_between_same_label_clusters() -> None:
    service = _build_service()

    embedding = [1.0, 0.0]
    centroids = {
        "applied": [
            (1, [1.0, 0.0]),
            (2, [0.97, 0.24]),
        ],
        "denied": [
            (1, [0.6, 0.8]),
        ],
    }

    result = service.classify_by_centroid(
        embedding,
        centroids,
        threshold=0.0,
        margin=0.05,
    )

    assert result[0] == "applied"
    assert result[1] == 1
    assert result[2] == pytest.approx(1.0, rel=1e-6)
    assert result[3] == pytest.approx(0.4, rel=1e-6)
    assert result[4] is None


def test_classify_by_centroid_applies_margin_between_labels() -> None:
    service = _build_service()

    embedding = [1.0, 0.0]
    centroids = {
        "applied": [
            (1, [1.0, 0.0]),
        ],
        "denied": [
            (1, [0.98, 0.199]),
        ],
    }

    result = service.classify_by_centroid(
        embedding,
        centroids,
        threshold=0.0,
        margin=0.05,
    )

    assert result[0] == "other"
    assert result[1] is None
    assert result[3] == pytest.approx(0.02, rel=1e-6)
    assert result[4] == "denied"
