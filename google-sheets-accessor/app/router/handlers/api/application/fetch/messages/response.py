from dataclasses import dataclass

from app.router.handlers.api.messages.base_api_response import BaseAPIResponse


@dataclass
class ApplicationFetchResponseData:
    applications_saved_amount: int = 0
    salaries_saved_amount: int = 0


@dataclass
class ApplicationFetchResponse(BaseAPIResponse):
    data: ApplicationFetchResponseData | None = None
