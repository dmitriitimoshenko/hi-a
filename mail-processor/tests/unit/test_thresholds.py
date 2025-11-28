from app.enums import EmailLabel
from app.thresholds import load_thresholds, resolve_threshold


def test_load_thresholds_returns_defaults_for_all_labels() -> None:
    thresholds = load_thresholds(0.6, raw_value="")

    for label in EmailLabel:
        assert thresholds[label.value] == 0.6


def test_load_thresholds_applies_overrides_case_insensitive() -> None:
    raw_value = "applied:0.8, OFFER:0.75"

    thresholds = load_thresholds(0.6, raw_value=raw_value)

    assert thresholds[EmailLabel.APPLIED.value] == 0.8
    assert thresholds[EmailLabel.OFFER.value] == 0.75
    assert thresholds[EmailLabel.DENIED.value] == 0.6


def test_resolve_threshold_uses_fallback_for_missing_label() -> None:
    thresholds = {EmailLabel.APPLIED.value: 0.7}

    applied_threshold = resolve_threshold(
        EmailLabel.APPLIED.value,
        thresholds,
        0.6,
    )
    pending_threshold = resolve_threshold(
        EmailLabel.PENDING.value,
        thresholds,
        0.6,
    )

    assert applied_threshold == 0.7
    assert pending_threshold == 0.6
