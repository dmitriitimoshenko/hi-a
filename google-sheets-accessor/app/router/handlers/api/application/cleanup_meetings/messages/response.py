from dataclasses import dataclass

from app.router.handlers.api.messages.base_api_response import BaseAPIResponse


@dataclass
class CleanupMeetingsResponseData:
    rows_reset: int


@dataclass
class CleanupMeetingsResponse(BaseAPIResponse):
    data: CleanupMeetingsResponseData | None = None
