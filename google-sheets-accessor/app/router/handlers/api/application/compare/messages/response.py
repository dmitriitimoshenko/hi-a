from dataclasses import dataclass, field
from typing import Any

from app.router.handlers.api.messages.base_api_response import BaseAPIResponse


@dataclass
class ApplicationDiffChange:
    field: str
    sheet_value: Any
    db_value: Any
    message: str


@dataclass
class ApplicationDiffEntry:
    application_id: int | None
    row_id: int
    company: str | None
    role_title: str | None
    differences: list[ApplicationDiffChange] = field(default_factory=list)
    sheet_payload: dict[str, Any] = field(default_factory=dict)
    db_snapshot: dict[str, Any] | None = None
    errors: list[str] = field(default_factory=list)


@dataclass
class ApplicationDiffResponseData:
    rows_checked: int
    rows_with_differences: int
    differences: list[ApplicationDiffEntry] = field(default_factory=list)


@dataclass
class ApplicationDiffResponse(BaseAPIResponse):
    data: ApplicationDiffResponseData | None = None
