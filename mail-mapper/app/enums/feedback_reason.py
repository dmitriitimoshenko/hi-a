from enum import Enum


class FeedbackReason(str, Enum):
    EMAIL_MISCLASSIFIED = "email_misclassified"
    APPLICATION_MISMATCH = "application_mismatch"
    OTHER_REASON = "other_reason"
