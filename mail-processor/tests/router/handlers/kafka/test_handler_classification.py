from __future__ import annotations

from contextlib import nullcontext

from app.enums import EmailLabel
from app.router.handlers.kafka.handler import (
    InboundIcsFile,
    InboundMailMessage,
    KafkaNewMailHandler,
    ClusterCentroid,
)


class DummyConfig:
    def __init__(
        self,
        thresholds: dict[str, float],
        *,
        default_threshold: float = 0.6,
        margin: float = 0.0,
    ) -> None:
        self.CLASSIFY_BY_CENTROID_THRESHOLDS = thresholds
        self.CLASSIFY_BY_CENTROID_THRESHOLD = default_threshold
        self.CLASSIFY_BY_CENTROID_MARGIN = margin


class FakeOpenAIService:
    def __init__(
        self,
        response: tuple[
            str,
            int | None,
            float,
            float | None,
            str | None,
            dict[str, dict[int, float]],
        ],
    ) -> None:
        self._response = response

    def get_embedding(self, *_: object, **__: object) -> list[float]:
        raise NotImplementedError

    def classify_by_centroid(
        self, *_: object, **__: object
    ) -> tuple[
        str,
        int | None,
        float,
        float | None,
        str | None,
        dict[str, dict[int, float]],
    ]:
        return self._response


def _build_handler(
    response: tuple[
        str,
        int | None,
        float,
        float | None,
        str | None,
        dict[str, dict[int, float]],
    ],
    thresholds: dict[str, float],
    *,
    default_threshold: float = 0.6,
) -> KafkaNewMailHandler:
    config = DummyConfig(thresholds, default_threshold=default_threshold)
    service = FakeOpenAIService(response)

    handler = KafkaNewMailHandler(
        config=config,
        openai_service=service,
        db_factory=lambda: nullcontext(None),
    )

    return handler


def _build_message() -> InboundMailMessage:
    return InboundMailMessage(
        subject="subject",
        body="body",
        sender_email="from@example.com",
        sender_name="Sender",
        recipient_name=None,
        recipient_email=None,
        content_type="text/plain",
        ics_files=[],
    )


def _build_handler_for_meeting_label() -> KafkaNewMailHandler:
    response = ("other", None, 0.0, None, None, {})

    return _build_handler(response, {})


def test_classify_email_returns_result_when_score_above_threshold() -> None:
    response = ("applied", 1, 0.85, None, None, {"applied": {1: 0.85}})
    thresholds = {EmailLabel.APPLIED.value: 0.7}

    handler = _build_handler(response, thresholds)
    message = _build_message()

    centroids = {
        "applied": [
            ClusterCentroid(
                label="applied",
                cluster_id=1,
                embedding=[0.1],
                threshold=0.8,
            )
        ]
    }

    result = handler._classify_email([0.1], centroids, message)

    assert result is not None
    assert result.label == EmailLabel.APPLIED
    assert result.cluster_id == 1
    assert result.threshold == 0.8
    assert result.threshold_source == "cluster"


def test_classify_email_returns_none_when_score_below_threshold() -> None:
    response = ("applied", 1, 0.72, None, None, {"applied": {1: 0.72}})
    thresholds = {EmailLabel.APPLIED.value: 0.65}

    handler = _build_handler(response, thresholds)
    message = _build_message()

    centroids = {
        "applied": [
            ClusterCentroid(
                label="applied",
                cluster_id=1,
                embedding=[0.1],
                threshold=0.8,
            )
        ]
    }

    result = handler._classify_email([0.1], centroids, message)

    assert result is None


def test_classify_email_uses_threshold_for_derived_meeting_label() -> None:
    ics_file = InboundIcsFile(
        filename="invite.ics",
        content_type="text/calendar",
        disposition=None,
        method="CANCEL",
        size=10,
        content=None,
    )
    response = (
        "meeting_action",
        2,
        0.9,
        None,
        None,
        {"meeting_action": {2: 0.9}},
    )
    thresholds = {
        EmailLabel.MEETING_ACTION.value: 0.5,
        EmailLabel.MEETING_CNCL.value: 0.95,
    }

    handler = _build_handler(response, thresholds)
    message = InboundMailMessage(
        subject="subject",
        body="body",
        sender_email="from@example.com",
        sender_name="Sender",
        recipient_name=None,
        recipient_email=None,
        content_type="text/plain",
        ics_files=[ics_file],
    )

    centroids = {
        "meeting_action": [
            ClusterCentroid(
                label="meeting_action",
                cluster_id=2,
                embedding=[0.1],
                threshold=None,
            )
        ]
    }

    result = handler._classify_email([0.1], centroids, message)

    assert result is None


def test_derive_meeting_label_detects_cancellation_in_request_summary() -> None:
    handler = _build_handler_for_meeting_label()

    ics_file = InboundIcsFile(
        filename="meeting.ics",
        content_type="text/calendar",
        disposition=None,
        method="REQUEST",
        size=10,
        content=(
            "BEGIN:VCALENDAR\nMETHOD:REQUEST\nBEGIN:VEVENT\n"
            "SUMMARY:Canceled: Meeting\nDESCRIPTION:CANCELLATION REASON: client\n"
            "END:VEVENT\nEND:VCALENDAR"
        ),
    )

    label = handler._derive_meeting_label([ics_file])

    assert label == EmailLabel.MEETING_CNCL


def test_derive_meeting_label_detects_sequence_zero_as_creation() -> None:
    handler = _build_handler_for_meeting_label()

    ics_file = InboundIcsFile(
        filename="meeting.ics",
        content_type="text/calendar",
        disposition=None,
        method="REQUEST",
        size=10,
        content=(
            "BEGIN:VCALENDAR\nMETHOD:REQUEST\nBEGIN:VEVENT\n"
            "SEQUENCE:0\nEND:VEVENT\nEND:VCALENDAR"
        ),
    )

    label = handler._derive_meeting_label([ics_file])

    assert label == EmailLabel.MEETING_CRT


def test_derive_meeting_label_detects_sequence_non_zero_as_update() -> None:
    handler = _build_handler_for_meeting_label()

    ics_file = InboundIcsFile(
        filename="meeting.ics",
        content_type="text/calendar",
        disposition=None,
        method="REQUEST",
        size=10,
        content=(
            "BEGIN:VCALENDAR\nMETHOD:REQUEST\nBEGIN:VEVENT\n"
            "SEQUENCE:2\nEND:VEVENT\nEND:VCALENDAR"
        ),
    )

    label = handler._derive_meeting_label([ics_file])

    assert label == EmailLabel.MEETING_UPD
