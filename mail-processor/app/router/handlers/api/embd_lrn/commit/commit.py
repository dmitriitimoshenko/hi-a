import logging

from fastapi import APIRouter, Depends

from .messages.response import EmbdLrnCommitResponse
from app.enums.base_api_response_status import BaseAPIResponseStatus
from app.services.embd_lrn import EmbdLrnService, get_embd_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["embd_lrn", "commit"],
    prefix="/embd_lrn",
)


@router.post("/commit", response_model=EmbdLrnCommitResponse, status_code=202)
def commit(
    embd_lrn_service: EmbdLrnService = Depends(get_embd_service),
) -> EmbdLrnCommitResponse:
    try:
        embd_lrn_service.commit()
    except Exception as e:
        message = f"failed to commit: {e}"
        logger.error(message)

        error_response = EmbdLrnCommitResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=message,
        )

        return error_response

    response = EmbdLrnCommitResponse(
        msg="Learning committed successfully",
    )

    return response
