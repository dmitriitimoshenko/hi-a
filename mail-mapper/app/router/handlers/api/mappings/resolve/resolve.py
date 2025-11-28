import logging

from fastapi import APIRouter, Depends

from .messages.request import MailMappingRequest
from .messages.response import (
    MailMappingMatchDTO,
    MailMappingResponse,
    MailMappingResultDTO,
)
from app.enums.base_api_response_status import BaseAPIResponseStatus
from app.services.mapping import EmailRecord, MailMappingService, get_mail_mapping_service

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["mappings"],
    prefix="/mappings",
)


@router.post("/resolve", response_model=MailMappingResponse)
def resolve_mappings(
    payload: MailMappingRequest,
    service: MailMappingService = Depends(get_mail_mapping_service),
) -> MailMappingResponse:
    emails = [
        EmailRecord(
            id=email.id,
            label=email.label,
            created_at=email.created_at,
            sender_email=email.sender_email,
            sender_name=email.sender_name,
            embedding=email.embedding,
        )
        for email in payload.emails
    ]

    if not emails:
        response = MailMappingResponse(
            msg="No emails provided for mapping",
        )

        return response

    try:
        result = service.resolve_mappings(
            status=payload.status,
            emails=emails,
        )
    except Exception as e:
        message = f"Failed to resolve mappings: {e}"
        logger.error(message)

        error_response = MailMappingResponse(
            status=BaseAPIResponseStatus.ERROR,
            msg=message,
        )

        return error_response

    response_data = MailMappingResultDTO(
        matches=[
            MailMappingMatchDTO(
                email_id=item.email_id,
                application=item.application,
                score=item.score,
            )
            for item in result.matches
        ],
        skipped_email_ids=result.skipped_email_ids,
    )

    response = MailMappingResponse(
        msg="Mappings resolved",
        data=response_data,
    )

    return response
