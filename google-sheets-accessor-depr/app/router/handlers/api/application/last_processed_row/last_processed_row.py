import logging

from fastapi import APIRouter, Depends

from .messages import (
    ApplicationLastProcessedRowResponse,
    ApplicationLastProcessedRowResponseData,
)
from app.enums import BaseAPIResponseStatus
from app.service.application.service import ApplicationService, get_application_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["application", "last-processed-row"],
    prefix="/application",
)


@router.post(
    "/last-processed-row",
    response_model=ApplicationLastProcessedRowResponse,
    status_code=200,
)
def get_last_processed_row(
    application_service: ApplicationService = Depends(get_application_service),
) -> ApplicationLastProcessedRowResponse:
    try:
        last_processed_row = application_service.get_last_processed_row()
    except Exception as e:
        msg = f"Failed to get last processed row: {e}"
        logger.error(msg)

        response = ApplicationLastProcessedRowResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=msg,
            data=None,
        )

        return response

    data = ApplicationLastProcessedRowResponseData(
        last_processed_row=last_processed_row,
    )

    response = ApplicationLastProcessedRowResponse(
        status=BaseAPIResponseStatus.OK,
        msg="Successfully fetched last processed row",
        data=data,
    )

    return response

