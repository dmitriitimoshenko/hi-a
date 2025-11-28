from dataclasses import dataclass

from app.enums.base_api_response_status import BaseAPIResponseStatus
from app.router.handlers.api.messages.base_api_response import BaseAPIResponse


@dataclass
class HealthCheckResponse(BaseAPIResponse):
    status: BaseAPIResponseStatus = BaseAPIResponseStatus.OK
