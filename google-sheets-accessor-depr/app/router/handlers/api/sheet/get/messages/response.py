from dataclasses import dataclass

from app.router.handlers.api.messages.base_api_response import BaseAPIResponse


@dataclass
class SheetGetResponse(BaseAPIResponse):
    data: list[list[str]] | None = None
