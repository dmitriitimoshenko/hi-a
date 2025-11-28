from __future__ import annotations

from unittest.mock import MagicMock

from app.services.embd_lrn import EmbdLrnService


class DummyConfig:
    CLASSIFY_BY_CENTROID_COMMIT_BATCH_SIZE = 10
    CLASSIFY_BY_CENTROID_THRESHOLD = 0.55
    CLASSIFY_BY_CENTROID_THRESHOLDS = {"applied": 0.7}


def _build_service() -> EmbdLrnService:
    dummy_db = MagicMock()
    dummy_openai = MagicMock()

    service = EmbdLrnService(dummy_db, dummy_openai, DummyConfig())

    return service


def test_split_embeddings_respects_batch_size() -> None:
    service = _build_service()

    embeddings = [[float(i)] for i in range(5)]

    batches = list(service._split_embeddings(embeddings, 2))

    assert len(batches) == 3
    assert batches[0] == [[0.0], [1.0]]
    assert batches[1] == [[2.0], [3.0]]
    assert batches[2] == [[4.0]]


def test_calculate_cluster_threshold_returns_min_similarity() -> None:
    dummy_openai = MagicMock()
    dummy_openai.compare_vectors.side_effect = [0.81, 0.79, 0.84]
    dummy_db = MagicMock()

    service = EmbdLrnService(dummy_db, dummy_openai, DummyConfig())

    centroid = [0.1, 0.2]
    embeddings = [[0.3, 0.4], [0.5, 0.6], [0.7, 0.8]]

    threshold = service._calculate_cluster_threshold("applied", centroid, embeddings)

    assert threshold == 0.79
    dummy_openai.compare_vectors.assert_called()


def test_calculate_cluster_threshold_fallbacks_to_config() -> None:
    service = _build_service()

    threshold = service._calculate_cluster_threshold("denied", [0.1], [])

    assert threshold == DummyConfig.CLASSIFY_BY_CENTROID_THRESHOLD


def test_calculate_cluster_threshold_uses_label_specific_threshold() -> None:
    service = _build_service()

    threshold = service._calculate_cluster_threshold("applied", [0.1], [[0.1]])

    assert threshold == DummyConfig.CLASSIFY_BY_CENTROID_THRESHOLDS["applied"]
