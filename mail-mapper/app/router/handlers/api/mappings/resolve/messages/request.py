from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field

from app.enums.email_label import EmailLabel


class MailMappingEmail(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: int
    label: EmailLabel
    created_at: datetime = Field(...)
    sender_email: str
    sender_name: str
    embedding: list[float]


class MailMappingRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: EmailLabel
    emails: list[MailMappingEmail]
