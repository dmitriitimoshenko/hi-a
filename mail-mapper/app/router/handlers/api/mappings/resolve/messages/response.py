from dataclasses import dataclass
from typing import Any

from app.router.handlers.api.messages.base_api_response import BaseAPIResponse


@dataclass
class MailMappingMatchDTO:
    email_id: int
    application: dict[str, Any]
    score: float


@dataclass
class MailMappingResultDTO:
    matches: list[MailMappingMatchDTO]
    skipped_email_ids: list[int]


@dataclass
class MailMappingResponse(BaseAPIResponse):
    data: MailMappingResultDTO | None = None
