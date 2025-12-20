from app.enums import BaseAPIResponseStatus
from dataclasses import dataclass


@dataclass
class BaseAPIResponse:
    status: BaseAPIResponseStatus = BaseAPIResponseStatus.OK
    msg: str | None = None
