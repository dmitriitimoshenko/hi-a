import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import requests

from app.config import Config
from app.enums import BaseAPIResponseStatus, EmailLabel

DEFAULT_MAIL_MAPPER_URL = Config.MAIL_MAPPER_URL or "http://mail-mapper:8088"
DEFAULT_HTTP_TIMEOUT = Config.MAIL_MAPPER_TIMEOUT
API_VERSION_HEADER = {"X-API-Version": Config.API_VERSION}


@dataclass
class MailMapperEmailPayload:
    id: int
    label: EmailLabel
    created_at: datetime
    sender_email: str
    sender_name: str
    embedding: Iterable[float]

    def to_dict(self) -> dict[str, Any]:
        embedding = self._normalized_embedding()

        payload: dict[str, Any] = {
            "id": self.id,
            "label": self.label.value,
            "created_at": self.created_at.isoformat(),
            "sender_email": self.sender_email,
            "sender_name": self.sender_name,
            "embedding": embedding,
        }

        return payload

    def _normalized_embedding(self) -> list[float]:
        normalized = [float(value) for value in self.embedding]

        return normalized


@dataclass
class MailMapperMatch:
    email_id: int
    application: dict[str, Any]
    score: float


@dataclass
class MailMapperResult:
    matches: list[MailMapperMatch]
    skipped_email_ids: list[int]


class MailMapperClient:
    def __init__(
        self,
        base_url: str = DEFAULT_MAIL_MAPPER_URL,
        *,
        timeout: float = DEFAULT_HTTP_TIMEOUT,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._logger = logging.getLogger(__name__)

    def resolve_mappings(
        self,
        *,
        status: EmailLabel,
        emails: list[MailMapperEmailPayload],
    ) -> MailMapperResult:
        if not emails:
            result = MailMapperResult(matches=[], skipped_email_ids=[])

            return result

        endpoint = f"{self._base_url}/api/mappings/resolve"
        payload = {
            "status": status.value,
            "emails": [email.to_dict() for email in emails],
        }

        try:
            response = self._session.post(
                endpoint,
                json=payload,
                headers=API_VERSION_HEADER,
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            message = f"Failed to call mail-mapper: {e}"
            self._logger.error(message)
            raise ValueError(message)

        if response.status_code != 200:
            message = (
                f"Mail-mapper returned non-200 status: "
                f"{response.status_code} body={response.text}"
            )
            self._logger.error(message)
            raise ValueError(message)

        try:
            body = response.json()
        except ValueError as e:
            message = f"Failed to decode mail-mapper response: {e}"
            self._logger.error(message)
            raise ValueError(message)

        status_value = body.get("status")
        if status_value != BaseAPIResponseStatus.OK.value:
            message = f"Mail-mapper returned error status: {body}"
            self._logger.error(message)
            raise ValueError(message)

        data = body.get("data") or {}
        matches_raw = data.get("matches") or []
        skipped_ids_raw = data.get("skipped_email_ids") or []

        matches: list[MailMapperMatch] = []

        for item in matches_raw:
            try:
                email_id = int(item["email_id"])
                application = item["application"]
                score = float(item["score"])
            except (KeyError, TypeError, ValueError):
                self._logger.warning("Invalid match payload: %s", item)
                continue

            matches.append(
                MailMapperMatch(
                    email_id=email_id,
                    application=application,
                    score=score,
                )
            )

        skipped_ids: list[int] = []
        for entry in skipped_ids_raw:
            try:
                skipped_ids.append(int(entry))
            except (TypeError, ValueError):
                self._logger.warning("Invalid skipped id payload: %s", entry)

        result = MailMapperResult(matches=matches, skipped_email_ids=skipped_ids)

        return result

    def close(self) -> None:
        self._session.close()


def get_mail_mapper_client(
    base_url: str | None = None,
    *,
    timeout: float | None = None,
) -> MailMapperClient:
    resolved_base_url = base_url or DEFAULT_MAIL_MAPPER_URL
    resolved_timeout = timeout if timeout is not None else DEFAULT_HTTP_TIMEOUT

    client = MailMapperClient(
        base_url=resolved_base_url,
        timeout=resolved_timeout,
    )

    return client
