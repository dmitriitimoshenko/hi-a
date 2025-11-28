import logging
from typing import cast

import redis

from .config import RedisConfig, get_redis_config

logging.basicConfig(level=logging.INFO)


class RedisClient:
    def __init__(self, config: RedisConfig) -> None:
        self._config = config
        self._client = redis.Redis(
            host=config.host,
            port=config.port,
            db=config.db,
            password=config.password,
            decode_responses=config.decode_responses,
        )
        self._logger = logging.getLogger(__name__)

    def set(self, key: str, value: str, ex: int | None = None) -> bool:
        self._logger.debug("Setting key %s with value %s", key, value)
        result = self._client.set(name=key, value=value, ex=ex)

        return bool(result)

    def get(self, key: str) -> str | None:
        value = cast(str | None, self._client.get(name=key))
        self._logger.debug("Getting key %s: %s", key, value)

        return value

    def delete(self, key: str) -> int:
        self._logger.debug("Deleting key %s", key)
        deleted = cast(int, self._client.delete(key))

        return deleted

    def exists(self, key: str) -> bool:
        self._logger.debug("Checking existence of key %s", key)
        exists_raw = cast(int, self._client.exists(key))

        return exists_raw > 0

    def flush_all(self):
        self._client.flushall()


def get_redis_client(config: RedisConfig | None = None) -> RedisClient:
    cfg = config or get_redis_config()

    return RedisClient(cfg)
