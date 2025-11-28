from fastapi import APIRouter

from .messages.response import HealthCheckResponse
from app.enums.base_api_response_status import BaseAPIResponseStatus

router = APIRouter(tags=["tools"])


@router.get("/health-check", response_model=HealthCheckResponse)
def health_check() -> HealthCheckResponse:
    response = HealthCheckResponse(
        status=BaseAPIResponseStatus.OK,
        msg="Mail processor service is healthy",
    )

    return response
