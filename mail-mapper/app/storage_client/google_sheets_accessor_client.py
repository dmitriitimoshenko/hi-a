import logging
from typing import Any

import requests

from app.config import Config
from app.enums import ApplicationStatus

DEFAULT_GSA_BASE_URL = Config.GSA_BASE_URL
DEFAULT_HTTP_TIMEOUT = Config.GSA_TIMEOUT
API_VERSION_HEADER = {"X-API-Version": Config.API_VERSION}


class GoogleSheetsAccessorClient:
    def __init__(
        self,
        base_url: str = DEFAULT_GSA_BASE_URL,
        *,
        timeout: float = DEFAULT_HTTP_TIMEOUT,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._session = requests.Session()
        self._logger = logging.getLogger(__name__)

    def list_applications_without_reply(
        self,
        *,
        status_include: list[ApplicationStatus] | None = None,
        status_exclude: list[ApplicationStatus] | None = None,
    ) -> list[dict[str, Any]]:
        endpoint = f"{self._base_url}/api/application/list"
        payload = {
            "application_status_include": self._serialize_statuses(status_include),
            "application_status_exclude": self._serialize_statuses(status_exclude),
            "is_reply_email_received": False,
        }

        try:
            response = self._session.post(
                endpoint,
                json=payload,
                headers=API_VERSION_HEADER,
                timeout=self._timeout,
            )
        except requests.RequestException as e:
            self._logger.error("Failed to fetch applications from GSA: %s", e)

            return []

        if response.status_code != 200:
            self._logger.error(
                "Failed to fetch applications from GSA: status=%s body=%s",
                response.status_code,
                response.text,
            )

            return []

        raw_data = response.json().get("data", [])
        sanitized = self._sanitize_applications(raw_data)

        return sanitized

    def close(self) -> None:
        self._session.close()

    def _serialize_statuses(
        self,
        statuses: list[ApplicationStatus] | None,
    ) -> list[str] | None:
        if statuses is None:
            return None

        serialized = [status.value for status in statuses]

        return serialized

    def _sanitize_applications(self, items: list[Any]) -> list[dict[str, Any]]:
        sanitized: list[dict[str, Any]] = []

        for item in items:
            if not isinstance(item, dict):
                continue

            cleaned = {
                key: value
                for key, value in item.items()
                if not key.endswith("_email_received") and not key.endswith("_email_id")
            }
            sanitized.append(cleaned)

        return sanitized


def get_google_sheets_accessor_client(
    base_url: str | None = None,
    *,
    timeout: float | None = None,
) -> GoogleSheetsAccessorClient:
    resolved_base_url = base_url or DEFAULT_GSA_BASE_URL
    resolved_timeout = timeout if timeout is not None else DEFAULT_HTTP_TIMEOUT

    client = GoogleSheetsAccessorClient(
        base_url=resolved_base_url,
        timeout=resolved_timeout,
    )

    return client
