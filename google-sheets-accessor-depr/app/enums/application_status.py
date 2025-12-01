from enum import Enum


class ApplicationStatus(str, Enum):
    APPLIED = "applied"
    MEETING = "meeting"
    OFFER = "offer"
    DENIED = "denied"
    PENDING = "pending"

    @classmethod
    def is_valid(cls, value: str) -> bool:
        return value in cls._value2member_map_

    @classmethod
    def get_all(cls) -> list[str]:
        return [member.value for member in cls]
