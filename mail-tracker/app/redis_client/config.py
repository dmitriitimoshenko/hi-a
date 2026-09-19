import os

from dataclasses import dataclass

DEFAULT_HOST = "redis"
DEFAULT_PORT = 6379
DEFAULT_DB = 0


def _int_from_env(name: str, fallback: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return fallback

    try:
        value = int(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer") from e

    return value


@dataclass(slots=True)
class RedisConfig:
    host: str = ""
    port: int = 0
    db: int = -1
    password: str | None = None
    decode_responses: bool = True

    def __post_init__(self) -> None:
        if not self.host:
            self.host = os.getenv("REDIS_HOST", DEFAULT_HOST)
        if not self.port:
            self.port = _int_from_env("REDIS_PORT", DEFAULT_PORT)
        if self.db < 0:
            self.db = _int_from_env("REDIS_DB", DEFAULT_DB)
        if self.password is None:
            self.password = os.getenv("REDIS_PASSWORD") or None
