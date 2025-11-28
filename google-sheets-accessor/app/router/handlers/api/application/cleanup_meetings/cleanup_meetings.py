import logging

from fastapi import APIRouter, Depends

from .messages.request import CleanupMeetingsRequest
from .messages.response import CleanupMeetingsResponse, CleanupMeetingsResponseData
from app.enums import BaseAPIResponseStatus
from app.service.application.service import ApplicationService, get_application_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["application", "cleanup-meetings"],
    prefix="/application",
)


@router.post(
    "/cleanup-meetings",
    response_model=CleanupMeetingsResponse,
    status_code=200,
)
def cleanup_meetings(
    request: CleanupMeetingsRequest,
    application_service: ApplicationService = Depends(get_application_service),
) -> CleanupMeetingsResponse:
    try:
        rows_reset = application_service.cleanup_overdue_meetings(
            request.id,
            request.sheet_page,
        )
    except Exception as e:
        msg = f"Failed to cleanup meetings: {e}"
        logger.error(msg)

        return CleanupMeetingsResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=msg,
            data=None,
        )

    return CleanupMeetingsResponse(
        status=BaseAPIResponseStatus.OK,
        msg="Cleanup completed",
        data=CleanupMeetingsResponseData(
            rows_reset=rows_reset,
        ),
    )
