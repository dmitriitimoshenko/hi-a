from __future__ import annotations

import logging
import os

from app.enums.email_label import EmailLabel

THRESHOLDS_ENV_VAR = "CLASSIFY_BY_CENTROID_THRESHOLDS"
PAIR_DELIMITER = ","
KV_DELIMITER = ":"


def build_default_thresholds(default_threshold: float) -> dict[str, float]:
    thresholds = {label.value.lower(): default_threshold for label in EmailLabel}

    return thresholds


def _parse_thresholds(raw: str) -> dict[str, float]:
    parsed: dict[str, float] = {}

    for chunk in raw.split(PAIR_DELIMITER):
        item = chunk.strip()
        if not item:
            continue

        if KV_DELIMITER not in item:
            logging.warning("Skipping invalid threshold entry: %s", item)

            continue

        label_part, value_part = item.split(KV_DELIMITER, 1)
        label = label_part.strip().lower()
        value_str = value_part.strip()

        if not EmailLabel.is_valid(label):
            logging.warning("Skipping unknown label in thresholds: %s", label)

            continue

        try:
            parsed[label] = float(value_str)
        except ValueError:
            logging.warning(
                "Skipping threshold for label %s due to invalid value: %s",
                label,
                value_str,
            )

            continue

    return parsed


def load_thresholds(
    default_threshold: float,
    *,
    env_var: str = THRESHOLDS_ENV_VAR,
    raw_value: str | None = None,
) -> dict[str, float]:
    raw = raw_value if raw_value is not None else os.getenv(env_var)

    thresholds = build_default_thresholds(default_threshold)

    if not raw:
        return thresholds

    parsed = _parse_thresholds(raw)
    thresholds.update(parsed)

    return thresholds


def resolve_threshold(
    label: str,
    thresholds: dict[str, float],
    fallback: float,
) -> float:
    key = label.lower()

    return thresholds.get(key, fallback)
