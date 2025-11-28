import logging
import math

from typing import Iterable, Sequence

from app.integrations.open_ai.client.client import OpenAIClient, get_openai_client

logging.basicConfig(level=logging.INFO)


class OpenAIService:
    def __init__(self, client: OpenAIClient):
        self._client = client
        self._logger = logging.getLogger(__name__)

    def get_embedding(
        self, text: str, model: str = "text-embedding-3-small"
    ) -> list[float]:
        response = self._client.get_embedding(text, model)
        return response.data[0].embedding

    def get_embeddings(
        self, texts: list[str], model: str = "text-embedding-3-small"
    ) -> list[list[float]]:
        response = self._client.get_embedding(texts, model)
        return [item.embedding for item in response.data]

    def _l2_normalize(self, v: Sequence[float]) -> list[float]:
        s = math.sqrt(sum(x * x for x in v))
        if s == 0.0:
            return [0.0 for _ in v]
        return [x / s for x in v]

    def get_embedding_centroid(
        self, vectors: Iterable[Sequence[float]], *, normalize_each: bool = True
    ) -> list[float]:
        vectors = [list(v) for v in vectors if v is not None]
        if not vectors:
            raise ValueError("get_embedding_centroid: empty input")

        dim = len(vectors[0])
        if any(len(v) != dim for v in vectors):
            raise ValueError(
                "get_embedding_centroid: vectors have different dimensions"
            )

        if normalize_each:
            vectors = [self._l2_normalize(v) for v in vectors]

        acc = [0.0] * dim
        for v in vectors:
            for i in range(dim):
                acc[i] += v[i]

        n = float(len(vectors))
        centroid = [x / n for x in acc]

        return centroid

    def _cosine_sim(self, a: Sequence[float], b: Sequence[float]) -> float:
        return sum(x * y for x, y in zip(a, b)) / (
            math.sqrt(sum(x * x for x in a)) * math.sqrt(sum(y * y for y in b)) or 1.0
        )

    def classify_by_centroid(
        self,
        emb: list[float],
        centroids: dict[str, list[tuple[int, list[float]]]],
        *,
        normalize: bool = True,
        threshold: float | None = 0.55,
        margin: float | None = 0.05,
    ) -> tuple[str, int | None, float, float | None, str | None, dict[str, dict[int, float]]]:
        if not centroids:
            self._logger.warning(
                "Centroids are missing, cannot classify items, skipping!"
            )
            return "other", None, 0.0, None, None, {}

        if normalize:
            emb = self._l2_normalize(emb)
            logging.info(
                "Normalized input embedding: emb len:%d (%s)", len(emb), type(emb)
            )
            normalized_centroids: dict[str, list[tuple[int, list[float]]]] = {}
            for label, items in centroids.items():
                normalized_centroids[label] = [
                    (cluster_id, self._l2_normalize(vector))
                    for cluster_id, vector in items
                ]

            cents = normalized_centroids
            logging.info(
                "Normalized centroids: labels=%d", len(normalized_centroids)
            )
        else:
            cents = centroids

        scores: dict[str, dict[int, float]] = {}
        label_best: dict[str, tuple[int, float]] = {}

        for label, items in cents.items():
            label_scores: dict[int, float] = {}

            for cluster_id, vector in items:
                score = self._cosine_sim(emb, vector)
                label_scores[cluster_id] = score

            if label_scores:
                scores[label] = label_scores
                best_cluster_id, best_cluster_score = max(
                    label_scores.items(), key=lambda item: item[1]
                )
                label_best[label] = (best_cluster_id, best_cluster_score)

        if not label_best:
            return "other", None, 0.0, None, None, {}

        best_label, (best_cluster_id, best_score) = max(
            label_best.items(), key=lambda item: item[1][1]
        )

        if threshold is not None and best_score < threshold:
            return "other", None, best_score, None, None, scores

        top_two_delta: float | None = None
        could_be: str | None = None

        if margin is not None:
            competitors = {
                label: score for label, (_, score) in label_best.items() if label != best_label
            }

            if competitors:
                could_be, competitor_score = max(
                    competitors.items(), key=lambda item: item[1]
                )
                top_two_delta = best_score - competitor_score

                if top_two_delta < margin:
                    return "other", None, best_score, top_two_delta, could_be, scores

        return best_label, best_cluster_id, best_score, top_two_delta, None, scores

    def compare_vectors(
        self,
        a: Sequence[float],
        b: Sequence[float],
        *,
        normalize: bool = True,
        metric: str = "cosine",
    ) -> float:
        if normalize:
            a = self._l2_normalize(a)
            b = self._l2_normalize(b)

        if metric == "cosine":
            return self._cosine_sim(a, b)
        elif metric == "dot":
            return sum(x * y for x, y in zip(a, b))
        else:
            raise ValueError(f"Unknown metric: {metric}")


def get_openai_service(client: OpenAIClient | None = None) -> OpenAIService:
    resolved_client = client or get_openai_client()

    return OpenAIService(resolved_client)


def provide_openai_service() -> OpenAIService:
    return get_openai_service()
