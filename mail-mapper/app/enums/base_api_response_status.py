from enum import Enum


class BaseAPIResponseStatus(str, Enum):
    OK = "ok"
    ERROR = "error"

    @classmethod
    def is_valid(cls, value: str) -> bool:
        return value in cls._value2member_map_
