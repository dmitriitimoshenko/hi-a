import logging

from fastapi import APIRouter, Depends

from .messages.response import MailsInterestingSubmitResponse
from app.enums.base_api_response_status import BaseAPIResponseStatus
from app.services.mail import MailService, get_mail_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["mails", "interesting", "submit"],
    prefix="/mails/interesting",
)


@router.post("/submit", response_model=MailsInterestingSubmitResponse, status_code=201)
def submit(
    mail_service: MailService = Depends(get_mail_service),
) -> MailsInterestingSubmitResponse:
    try:
        submission_result = mail_service.submit_interesting()
    except Exception as e:
        message = f"Failed to submit: {e}"
        logger.error(message)

        error_response = MailsInterestingSubmitResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=message,
        )

        return error_response

    if not submission_result:
        response = MailsInterestingSubmitResponse(
            msg="Submitted NOTHING successfully",
        )

        return response

    response = MailsInterestingSubmitResponse(
        msg="Submitted successfully",
        data=submission_result,
    )

    return response
