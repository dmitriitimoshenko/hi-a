from dataclasses import dataclass

from app.enums.base_api_response_status import BaseAPIResponseStatus


@dataclass
class BaseAPIResponse:
    status: BaseAPIResponseStatus = BaseAPIResponseStatus.OK
    msg: str | None = None
