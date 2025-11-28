from dataclasses import dataclass

from app.router.handlers.api.messages.base_api_response import BaseAPIResponse
from app.services.mail import EmailToApplicationMappingDTO
from app.enums.email_label import EmailLabel


@dataclass
class MailsInterestingSubmitResponse(BaseAPIResponse):
    data: (
        None
        | dict[EmailLabel, tuple[list[EmailToApplicationMappingDTO] | None, int, int]]
    ) = None
