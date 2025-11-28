import logging

from fastapi import APIRouter, Depends

from .messages import ApplicationUpdateResponse
from app.enums import BaseAPIResponseStatus
from app.service.application.service import ApplicationService, get_application_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["application", "update"],
    prefix="/application",
)


@router.post(
    "/update-internal",
    response_model=ApplicationUpdateResponse,
    status_code=200,
)
def update(
    request: dict,
    application_service: ApplicationService = Depends(get_application_service),
) -> ApplicationUpdateResponse:
    try:
        application_service.update(request)
        logger.info(f"Updated applications")
    except Exception as e:
        msg = f"Failed to update applications: {e}"
        logger.error(msg)
        return ApplicationUpdateResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=msg,
        )

    return ApplicationUpdateResponse(
        status=BaseAPIResponseStatus.OK,
        msg="Successfully updated applications",
    )
