import logging

from fastapi import APIRouter, Depends

from .messages.response import EmbdLrnCreateResponse
from .messages.request import EmbdLrnCreateRequest
from app.enums.email_label import EmailLabel
from app.enums.base_api_response_status import BaseAPIResponseStatus
from app.services.embd_lrn import EmbdLrnService, get_embd_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["embd_lrn", "create"],
    prefix="/embd_lrn",
)


@router.post("/create", response_model=EmbdLrnCreateResponse, status_code=201)
def create(
    request: EmbdLrnCreateRequest,
    embd_lrn_service: EmbdLrnService = Depends(get_embd_service),
) -> EmbdLrnCreateResponse:
    label = request.label
    items = request.items

    if not EmailLabel.is_valid(label):
        message = f"label is invalid: {label}"
        logger.warning(message)

        error_response = EmbdLrnCreateResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=message,
        )

        return error_response

    if not items:
        message = "\"items\" must be non-empty"
        logger.warning("Received empty items list")

        error_response = EmbdLrnCreateResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=message,
        )

        return error_response

    if not isinstance(items, list):
        message = "\"items\" must be a list"
        logger.warning("Received items of type: %s", type(items))

        error_response = EmbdLrnCreateResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=message,
        )

        return error_response

    try:
        embd_lrn_service.create_with_label(items, EmailLabel(label))
    except Exception as e:
        message = f"failed to create_with_label: {e}"
        logger.error(message)

        error_response = EmbdLrnCreateResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=message,
        )

        return error_response

    response = EmbdLrnCreateResponse(
        data={"created_amount": len(items)},
        msg="New data added successfully",
    )

    return response
