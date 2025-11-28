from dataclasses import dataclass


@dataclass(slots=True)
class RedisConfig:
    host: str = "redis-mail-tracker"
    port: int = 6379
    db: int = 0
    password: str | None = None
    decode_responses: bool = True
