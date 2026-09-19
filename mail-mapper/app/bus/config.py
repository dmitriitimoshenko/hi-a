import os

from dataclasses import dataclass

DEFAULT_DB = 2
DEFAULT_MAX_LEN = 10000
DEFAULT_BATCH_COUNT = 10
DEFAULT_RETRIES = 30


@dataclass(slots=True)
class BusConfig:
    redis_url: str
    consumer_id: str
    db: int = DEFAULT_DB
    max_len: int = DEFAULT_MAX_LEN
    batch_count: int = DEFAULT_BATCH_COUNT
    retries: int = DEFAULT_RETRIES
    start_at_oldest: bool = False


def _int_from_env(name: str, fallback: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return fallback

    try:
        value = int(raw)
    except ValueError as e:
        raise ValueError(f"{name} must be an integer") from e

    return value


def get_bus_config() -> BusConfig:
    config = BusConfig(
        redis_url=os.getenv("REDIS_URL", ""),
        consumer_id=os.getenv("STREAM_CONSUMER_ID", ""),
        db=_int_from_env("STREAM_DB", DEFAULT_DB),
        max_len=_int_from_env("STREAM_MAX_LEN", DEFAULT_MAX_LEN),
        start_at_oldest=os.getenv("STREAM_START_AT_OLDEST") == "true",
    )

    return config
