from dataclasses import dataclass

from app.router.handlers.api.messages.base_api_response import BaseAPIResponse


@dataclass
class ApplicationListResponse(BaseAPIResponse):
    data: list[dict] | None = None
