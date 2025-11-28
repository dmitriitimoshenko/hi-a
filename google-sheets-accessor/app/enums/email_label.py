from enum import Enum


class EmailLabel(str, Enum):
    APPLIED = "applied"
    MEETING_INV = "meeting_inv"
    MEETING_CRT = "meeting_crt"
    MEETING_UPD = "meeting_upd"
    MEETING_CNCL = "meeting_cncl"
    # MEETING_UNDEF = "meeting_undef"
    OFFER = "offer"
    DENIED = "denied"
    PENDING = "pending"

    @classmethod
    def is_valid(cls, value: str) -> bool:
        return value in cls._value2member_map_

    @classmethod
    def get_all(cls) -> list[str]:
        return [member.value for member in cls]
