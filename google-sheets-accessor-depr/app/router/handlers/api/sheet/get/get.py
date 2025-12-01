import logging

from fastapi import APIRouter, Depends

from .messages import SheetGetRequest, SheetGetResponse
from app.integrations.google_sheets_client.service.service import (
    SheetsService,
    get_sheets_service,
)
from app.enums import BaseAPIResponseStatus

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["sheet", "get"],
    prefix="/sheet",
)


@router.post("/get", response_model=SheetGetResponse, status_code=200)
def get(
    request: SheetGetRequest,
    sheets_service: SheetsService = Depends(get_sheets_service),
) -> SheetGetResponse:
    id = request.id
    range = request.range
    logger.debug(f"Request payload: id={id}, range={range}")

    try:
        vals = sheets_service.get_values(id, range)
        logger.debug(
            f"Fetched {len(vals)} rows from Google Sheet: {id}, range: {range}"
        )
    except Exception as e:
        msg = f"Error fetching Google Sheet values: {e}"
        logger.error(msg)
        return SheetGetResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=msg,
            data=None,
        )

    return SheetGetResponse(
        status=BaseAPIResponseStatus.OK,
        msg="Successfully got data from a google sheet",
        data=vals,
    )
