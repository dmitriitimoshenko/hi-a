"""
Classify arbitrary text using stored centroids from the mail-processor DB.

Usage:
  python3.13 scripts/classify_text.py "your text here" [--threshold 0.65] [--margin 0.015] [--show-scores] [--db <DB_URL>]

Example:
  python3.13 scripts/classify_text.py "Schedule a meeting tomorrow" --show-scores --db "$DATABASE_MAIL_PROCESSOR_URL" --threshold 0.52 --margin 0.015

Reads DB URL and OpenAI API key from the environment (.env at repo root is picked up if present).
"""

from __future__ import annotations

import argparse
import os
import sys
import json
import urllib.request
import urllib.error
import math


def _get_embedding_via_http(
    api_key: str, text: str, *, model: str = "text-embedding-3-small"
) -> list[float]:
    sanitized_text = text.replace("\n", " ")

    url_base = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")
    url = f"{url_base}/embeddings"

    request_body = json.dumps({"model": model, "input": sanitized_text}).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=request_body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as http_error:
        error_body = (
            http_error.read().decode("utf-8", errors="ignore")
            if hasattr(http_error, "read")
            else str(http_error)
        )

        message = f"HTTP {http_error.code}: {error_body}"

        raise RuntimeError(message)
    except urllib.error.URLError as url_error:
        message = f"Network error: {url_error}"

        raise RuntimeError(message)

    try:
        embedding = response_payload["data"][0]["embedding"]

        return embedding
    except Exception:
        message = f"Unexpected response: {response_payload}"

        raise RuntimeError(message)


def _l2_normalize(vector: list[float]) -> list[float]:
    norm = math.sqrt(sum(value * value for value in vector))
    if norm == 0.0:
        zeros = [0.0 for _ in vector]

        return zeros

    normalized = [value / norm for value in vector]

    return normalized


def _cosine_sim(vector_a: list[float], vector_b: list[float]) -> float:
    denom_left = math.sqrt(sum(a_val * a_val for a_val in vector_a))
    denom_right = math.sqrt(sum(b_val * b_val for b_val in vector_b))
    denom_product = (denom_left * denom_right) or 1.0

    dot = sum(a_val * b_val for a_val, b_val in zip(vector_a, vector_b))
    cosine = dot / denom_product

    return cosine


def _classify_by_centroid(
    *,
    embedding: list[float],
    centroids: dict[str, list[tuple[int, list[float]]]],
    normalize: bool = True,
    threshold: float | None = 0.55,
    margin: float | None = 0.05,
    raw_text_for_boosting: str | None = None,
) -> tuple[
    str,
    int | None,
    float,
    float | None,
    str | None,
    dict[str, dict[int, float]],
]:
    if normalize:
        normalized_embedding = _l2_normalize(embedding)
        normalized_centroids: dict[str, list[tuple[int, list[float]]]] = {}
        for label, items in centroids.items():
            normalized_centroids[label] = [
                (cluster_id, _l2_normalize(vector)) for cluster_id, vector in items
            ]
    else:
        normalized_embedding = embedding
        normalized_centroids = centroids

    scores: dict[str, dict[int, float]] = {}
    flattened: list[tuple[str, int, float]] = []

    for label, items in normalized_centroids.items():
        label_scores: dict[int, float] = {}

        for cluster_id, vector in items:
            score = _cosine_sim(normalized_embedding, vector)
            label_scores[cluster_id] = score
            flattened.append((label, cluster_id, score))

        if label_scores:
            scores[label] = label_scores

    if not flattened:
        return "other", None, 0.0, None, None, {}

    flattened.sort(key=lambda item: item[2], reverse=True)
    best_label, best_cluster_id, best_score = flattened[0]

    if threshold is not None and best_score < threshold:
        return "other", None, best_score, None, None, scores

    top_two_delta: float | None = None
    could_be_label: str | None = None

    if margin is not None and len(flattened) >= 2:
        top_two_delta = flattened[0][2] - flattened[1][2]
        could_be_label = flattened[1][0]

        if top_two_delta < margin:
            return "other", None, best_score, top_two_delta, could_be_label, scores

    return best_label, best_cluster_id, best_score, top_two_delta, could_be_label, scores


def _load_env_from_dotenv_if_present() -> None:
    script_dir = os.path.abspath(os.path.dirname(__file__))
    candidates = [
        os.path.abspath(os.path.join(script_dir, "..", ".env")),
        os.path.abspath(os.path.join(script_dir, "..", "..", ".env")),
        os.path.abspath(os.path.join(script_dir, "..", "..", "..", ".env")),
    ]

    for env_path in candidates:
        if not os.path.exists(env_path):
            continue
        try:
            with open(env_path, "r", encoding="utf-8") as env_file:
                for raw_line in env_file:
                    line = raw_line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" not in line:
                        continue

                    key, value = line.split("=", 1)
                    key = key.strip()
                    value = value.strip().strip('"').strip("'")

                    os.environ.setdefault(key, value)
        except Exception:
            continue

        break


def _ensure_import_path() -> None:
    script_dir = os.path.abspath(os.path.dirname(__file__))

    candidates = [
        os.path.abspath(os.path.join(script_dir, "..", "mail-processor")),
        os.path.abspath(os.path.join(script_dir, "..")),
    ]

    for candidate in candidates:
        app_dir = os.path.join(candidate, "app")
        if os.path.isdir(app_dir):
            if candidate not in sys.path:
                sys.path.insert(0, candidate)

            break


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Classify text using centroid classifier"
    )
    parser.add_argument("text", help="Text to classify")
    parser.add_argument(
        "--threshold",
        type=float,
        default=None,
        help="Min similarity to accept any class",
    )
    parser.add_argument(
        "--margin",
        type=float,
        default=None,
        help="Top1-Top2 min margin; below => 'other'",
    )
    parser.add_argument(
        "--show-scores", action="store_true", help="Print per-label similarity scores"
    )
    parser.add_argument(
        "--db",
        dest="db_url",
        help="Override DATABASE_MAIL_PROCESSOR_URL for local runs",
    )

    parsed = parser.parse_args()

    return parsed


def main() -> int:
    _load_env_from_dotenv_if_present()
    _ensure_import_path()

    args = parse_args()
    if args.db_url:
        os.environ["DATABASE_MAIL_PROCESSOR_URL"] = args.db_url

    from app.config import Config
    from app.database import get_db_context
    from app.models import EmbdCntr

    config = Config()
    if args.threshold is None:
        args.threshold = config.CLASSIFY_BY_CENTROID_THRESHOLD
    if args.margin is None:
        args.margin = config.CLASSIFY_BY_CENTROID_MARGIN

    per_class_thresholds = config.CLASSIFY_BY_CENTROID_THRESHOLDS

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY is not set (check your .env)")

        return 2

    categorized_centroids: dict[str, list[tuple[int, list[float]]]] = {}
    cluster_thresholds: dict[str, dict[int, float | None]] = {}
    try:
        with get_db_context() as db_session:
            rows = db_session.query(EmbdCntr).all()
            if not rows:
                print("ERROR: No centroids found in DB (table embd_cntr is empty)")

                return 3

            for row in rows:
                if row.embedding is None:
                    print(f"WARN: centroid id={row.id} has no embedding; skipping")
                    continue

                labeled_centroids = categorized_centroids.setdefault(row.label, [])
                labeled_centroids.append((row.cluster_id, row.embedding))

                thresholds_map = cluster_thresholds.setdefault(row.label, {})
                thresholds_map[row.cluster_id] = row.threshold
    except Exception as db_error:
        print(f"ERROR: failed to query DB: {db_error}")
        print(
            "Hint: if running outside Docker, pass --db 'postgresql+psycopg://user:pass@localhost:5432/postgres'"
        )

        return 5

    model = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
    try:
        embedding = _get_embedding_via_http(api_key, args.text, model=model)
    except Exception as embed_error:
        print(f"ERROR: failed to get embedding: {embed_error}")

        return 4

    (
        label,
        cluster_id,
        score,
        top_two_delta,
        could_be,
        scores,
    ) = _classify_by_centroid(
        embedding=embedding,
        centroids=categorized_centroids,
        threshold=None,
        margin=args.margin,
        raw_text_for_boosting=args.text,
    )

    used_threshold = args.threshold
    threshold_source = "default"

    if label != "other" and cluster_id is not None:
        cluster_threshold = cluster_thresholds.get(label, {}).get(cluster_id)
        if cluster_threshold is not None:
            used_threshold = cluster_threshold
            threshold_source = "cluster"
        else:
            class_threshold = per_class_thresholds.get(label.lower())
            if class_threshold is not None:
                used_threshold = class_threshold
                threshold_source = "class"

        if used_threshold is not None and score < used_threshold:
            could_be = label if could_be is None else could_be
            label = "other"
            cluster_id = None

    if label == "meeting_action":
        label = "meeting_inv"

    if could_be == "meeting_action":
        could_be = "meeting_inv"

    print("label:", label)
    print("score:", round(score, 6))
    print("cluster_id:", cluster_id if cluster_id is not None else "-")
    print("threshold:", round(used_threshold, 6) if used_threshold is not None else "-")
    print("threshold_source:", threshold_source)
    if top_two_delta is not None:
        print("top_two_delta:", round(top_two_delta, 6))
    if could_be:
        print("could_be:", could_be)

    if args.show_scores:
        print("scores:")
        flattened_scores: list[tuple[str, int, float]] = []
        for label_key, cluster_scores in scores.items():
            for cluster_key, score_value in cluster_scores.items():
                flattened_scores.append((label_key, cluster_key, score_value))

        flattened_scores.sort(key=lambda item: item[2], reverse=True)
        for label_key, cluster_key, score_value in flattened_scores:
            print(f"  {label_key}#{cluster_key}: {score_value:.6f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
