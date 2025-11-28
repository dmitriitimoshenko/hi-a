from dataclasses import dataclass

from app.router.handlers.api.messages.base_api_response import BaseAPIResponse


@dataclass
class ApplicationLastProcessedRowResponseData:
    last_processed_row: int = 0


@dataclass
class ApplicationLastProcessedRowResponse(BaseAPIResponse):
    data: ApplicationLastProcessedRowResponseData | None = None

