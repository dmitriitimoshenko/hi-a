from dataclasses import dataclass

from app.enums.application_status import ApplicationStatus


@dataclass
class ApplicationListRequest:
    application_status_include: list[ApplicationStatus] | None
    application_status_exclude: list[ApplicationStatus] | None
    is_reply_email_received: bool
