from app.router.handlers.api.messages.base_api_response import BaseAPIResponse
from dataclasses import dataclass


@dataclass
class EmbdLrnCreateResponse(BaseAPIResponse):
    data: dict | None = None
