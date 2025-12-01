import logging

from fastapi import APIRouter, Depends

from .messages import (
    ApplicationFetchResponseData,
    ApplicationFetchResponse,
    ApplicationFetchRequest,
)
from app.enums import BaseAPIResponseStatus
from app.service.application.service import ApplicationService, get_application_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["application", "fetch"],
    prefix="/application",
)


@router.post("/fetch", response_model=ApplicationFetchResponse, status_code=200)
def fetch(
    request: ApplicationFetchRequest,
    application_service: ApplicationService = Depends(get_application_service),
) -> ApplicationFetchResponse:
    id = request.id
    range = request.sheet_range

    try:
        applications_saved_amount, salaries_saved_amount = application_service.fetch(
            id,
            range.sheet_page,
            range.start_ceil,
            range.end_ceil,
        )
    except Exception as e:
        msg = f"Failed to fetch applications: {e}"
        logger.error(msg)
        return ApplicationFetchResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=msg,
            data=None,
        )

    return ApplicationFetchResponse(
        status=BaseAPIResponseStatus.OK,
        msg="Successfully fetched applications",
        data=ApplicationFetchResponseData(
            applications_saved_amount=applications_saved_amount,
            salaries_saved_amount=salaries_saved_amount,
        ),
    )
