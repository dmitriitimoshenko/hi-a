import logging

from fastapi import APIRouter, Depends

from .messages.response import EmbdLrnLearnResponse
from app.enums.base_api_response_status import BaseAPIResponseStatus
from app.services.embd_lrn import EmbdLrnService, get_embd_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["embd_lrn", "learn"],
    prefix="/embd_lrn",
)


@router.post("/learn", response_model=EmbdLrnLearnResponse, status_code=201)
def learn(
    embd_lrn_service: EmbdLrnService = Depends(get_embd_service),
) -> EmbdLrnLearnResponse:
    try:
        embd_lrn_service.learn()
    except Exception as e:
        message = f"failed to learn: {e}"
        logger.error(message)

        error_response = EmbdLrnLearnResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=message,
        )

        return error_response

    response = EmbdLrnLearnResponse(
        msg="Learning initiated",
    )

    return response
