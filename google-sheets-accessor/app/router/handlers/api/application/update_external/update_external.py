import logging

from fastapi import APIRouter, Depends

from app.enums import BaseAPIResponseStatus
from app.service.application.service import ApplicationService, get_application_service

from .messages import (
    ApplicationUpdateExternalRequest,
    ApplicationUpdateExternalResponse,
)


logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["application", "update-external"],
    prefix="/application",
)


@router.post(
    "/update-external",
    response_model=ApplicationUpdateExternalResponse,
    status_code=200,
)
def update_external(
    request: ApplicationUpdateExternalRequest,
    application_service: ApplicationService = Depends(get_application_service),
) -> ApplicationUpdateExternalResponse:
    try:
        application_service.update_external(
            request.sheet_page,
            request.db_snapshot,
        )
    except Exception as e:
        msg = f"Failed to update Google Sheet from DB snapshot: {e}"
        logger.error(msg)

        return ApplicationUpdateExternalResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=msg,
        )

    return ApplicationUpdateExternalResponse(
        status=BaseAPIResponseStatus.OK,
        msg="Successfully updated Google Sheet from DB snapshot",
    )
