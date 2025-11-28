"""Mail mapper HTTP client factory."""

from .client import (
    MailMapperClient,
    MailMapperEmailPayload,
    MailMapperMatch,
    MailMapperResult,
    get_mail_mapper_client,
)

__all__ = [
    "MailMapperClient",
    "MailMapperEmailPayload",
    "MailMapperMatch",
    "MailMapperResult",
    "get_mail_mapper_client",
]
