import logging

from fastapi import APIRouter, Depends

from .messages import ApplicationListRequest, ApplicationListResponse
from app.enums import BaseAPIResponseStatus
from app.service.application.service import ApplicationService, get_application_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["application", "list"],
    prefix="/application",
)


@router.post("/list", response_model=ApplicationListResponse, status_code=200)
def list_(
    request: ApplicationListRequest,
    application_service: ApplicationService = Depends(get_application_service),
) -> ApplicationListResponse:
    application_status_include = request.application_status_include
    application_status_exclude = request.application_status_exclude

    is_reply_email_received = request.is_reply_email_received

    try:
        application_list = application_service.list_(
            application_status_include,
            application_status_exclude,
            is_reply_email_received,
        )
        logger.info(f"Listed {len(application_list)} applications")
    except Exception as e:
        msg = f"Failed to list applications: {e}"
        logger.error(msg)
        return ApplicationListResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=msg,
            data=None,
        )

    return ApplicationListResponse(
        status=BaseAPIResponseStatus.OK,
        msg="Successfully listed applications",
        data=application_list,
    )
