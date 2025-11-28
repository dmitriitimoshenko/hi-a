from __future__ import annotations

from datetime import datetime, timezone

from app.enums import EmailLabel
from app.services.mapping import EmailApplicationMatcher, EmailRecord, MailMappingService


class FakeOpenAIService:
    def compare_vectors(self, a, b, *, normalize: bool = True, metric: str = "cosine") -> float:
        return 1.0


class DummyStorageClient:
    def list_applications_without_reply(self, *, status_include, status_exclude):
        raise AssertionError("Storage access is not expected in this test")


class DummyMatcher:
    def match_batch(self, emails, applications, *, blocked_application_ids=None):
        raise AssertionError("Matcher access is not expected in this test")


class FakeQuery:
    def __init__(self, rows: list[tuple[int, int | None]]) -> None:
        self._rows = rows

    def where(self, *args, **kwargs):
        return self

    def all(self):
        return list(self._rows)


class FakeSession:
    def __init__(self, rows: list[tuple[int, int | None]]) -> None:
        self._rows = rows

    def query(self, *args, **kwargs):
        return FakeQuery(self._rows)


def _build_application(application_id: int, created_at: datetime) -> dict[str, object]:
    iso_created_at = created_at.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")

    application = {
        "id": application_id,
        "created_at": iso_created_at,
        "company": "Example Corp",
        "meta": {"contacts": "HR <hr@example.com>"},
        "embedding": [0.5, 0.5],
    }

    return application


def _build_email(email_id: int, created_at: datetime) -> EmailRecord:
    email = EmailRecord(
        id=email_id,
        label=EmailLabel.APPLIED,
        created_at=created_at,
        sender_email="hr@example.com",
        sender_name="HR",
        embedding=[0.5, 0.5],
    )

    return email


def test_load_blocked_application_ids_returns_unique_mapping():
    rows = [
        (1, 101),
        (1, 101),
        (1, 102),
        (2, None),
    ]
    session = FakeSession(rows)

    service = MailMappingService(
        storage_client=DummyStorageClient(),
        matcher=DummyMatcher(),
        db=session,
        min_match_score=0.5,
    )

    blocked = service._load_blocked_application_ids(email_ids=[1, 2, 3])

    assert blocked == {1: {101, 102}}


def test_match_batch_avoids_blocked_application():
    matcher = EmailApplicationMatcher(
        openai_service=FakeOpenAIService(),
        min_match_score=0.1,
    )

    created_at = datetime.now(timezone.utc)
    email = _build_email(1, created_at)
    first_application = _build_application(101, created_at)
    alternative_application = _build_application(102, created_at)

    result = matcher.match_batch(
        emails=[email],
        applications=[first_application, alternative_application],
        blocked_application_ids={email.id: {101}},
    )

    assert email.id in result.assignments
    assert result.assignments[email.id].application_id == 102


def test_match_batch_skips_when_all_candidates_blocked():
    matcher = EmailApplicationMatcher(
        openai_service=FakeOpenAIService(),
        min_match_score=0.1,
    )

    created_at = datetime.now(timezone.utc)
    email = _build_email(1, created_at)
    application = _build_application(101, created_at)

    result = matcher.match_batch(
        emails=[email],
        applications=[application],
        blocked_application_ids={email.id: {101}},
    )

    assert email.id not in result.assignments
