import logging

from fastapi import APIRouter, Depends

from app.enums import BaseAPIResponseStatus
from app.service.application.service import ApplicationService, get_application_service

from .messages import (
    ApplicationDiffChange,
    ApplicationDiffEntry,
    ApplicationDiffRequest,
    ApplicationDiffResponse,
    ApplicationDiffResponseData,
)


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["application", "diff"],
    prefix="/application",
)


@router.post("/diff", response_model=ApplicationDiffResponse, status_code=200)
def diff(
    request: ApplicationDiffRequest,
    application_service: ApplicationService = Depends(get_application_service),
) -> ApplicationDiffResponse:
    sheet_range = request.sheet_range

    try:
        rows_checked, rows_with_differences, diff_payloads = (
            application_service.log_differences_between_sheet_and_db(
                request.id,
                sheet_range.sheet_page,
                sheet_range.start_row,
                sheet_range.end_row,
            )
        )
    except ValueError as e:
        message = f"Failed to compare applications: {e}"
        logger.error(message)

        return ApplicationDiffResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=message,
            data=None,
        )

    differences = []

    for payload in diff_payloads:
        changes_payload = payload.get("differences", [])
        changes = [
            ApplicationDiffChange(
                field=change.get("field", ""),
                sheet_value=change.get("sheet_value"),
                db_value=change.get("db_value"),
                message=change.get("message", ""),
            )
            for change in changes_payload
            if isinstance(change, dict)
        ]

        entry = ApplicationDiffEntry(
            application_id=payload.get("application_id"),
            row_id=payload.get("row_id", 0),
            company=payload.get("company"),
            role_title=payload.get("role_title"),
            differences=changes,
            sheet_payload=payload.get("sheet_payload", {}),
            db_snapshot=payload.get("db_snapshot"),
            errors=payload.get("errors", []),
        )
        differences.append(entry)

    return ApplicationDiffResponse(
        status=BaseAPIResponseStatus.OK,
        msg="Comparison completed",
        data=ApplicationDiffResponseData(
            rows_checked=rows_checked,
            rows_with_differences=rows_with_differences,
            differences=differences,
        ),
    )
