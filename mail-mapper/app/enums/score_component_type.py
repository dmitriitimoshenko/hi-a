from enum import Enum


class ScoreComponentType(str, Enum):
    SIMILARITY = "similarity"
    TIME = "time"
    SENDER = "sender"
