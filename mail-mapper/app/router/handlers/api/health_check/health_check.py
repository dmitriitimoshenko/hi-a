from fastapi import APIRouter

from .messages.response import HealthCheckResponse

router = APIRouter(tags=["tools"])


@router.get("/health-check", response_model=HealthCheckResponse)
def health_check() -> HealthCheckResponse:
    response = HealthCheckResponse(
        msg="Mail mapper service is healthy",
    )

    return response
