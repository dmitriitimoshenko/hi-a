"""Enumerations used across the mail-mapper service."""

from .application_status import ApplicationStatus
from .base_api_response_status import BaseAPIResponseStatus
from .email_label import EmailLabel
from .feedback_reason import FeedbackReason
from .score_component_type import ScoreComponentType

__all__ = [
    "ApplicationStatus",
    "BaseAPIResponseStatus",
    "EmailLabel",
    "FeedbackReason",
    "ScoreComponentType",
]
