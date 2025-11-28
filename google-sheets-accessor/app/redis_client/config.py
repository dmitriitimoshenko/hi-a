from dataclasses import dataclass


@dataclass(slots=True)
class RedisConfig:
    host: str = "redis-google-sheets-accessor"
    port: int = 6379
    db: int = 0
    password: str | None = None
    decode_responses: bool = True


def get_redis_config() -> RedisConfig:
    return RedisConfig()
