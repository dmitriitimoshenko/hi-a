import redis
import logging
from .config import RedisConfig, get_redis_config

logging.basicConfig(level=logging.INFO)


class RedisClient:
    def __init__(self, config: RedisConfig):
        self._config = config
        self._client = redis.Redis(
            host=config.host,
            port=config.port,
            db=config.db,
            password=config.password,
            decode_responses=config.decode_responses,
        )
        self._logger = logging.getLogger(__name__)

    def set(self, key: str, value: str | bytes, ex: int | None = None) -> bool:
        self._logger.debug("Setting key %s with value %s", key, value)
        return self._client.set(name=key, value=value, ex=ex)

    def get(self, key: str) -> str | None:
        v = self._client.get(name=key)
        self._logger.debug("Getting key %s: %s", key, v)
        return v

    def delete(self, key: str) -> int:
        self._logger.debug("Deleting key %s", key)
        return self._client.delete(key)

    def exists(self, key: str) -> bool:
        self._logger.debug("Checking existence of key %s", key)
        return self._client.exists(key) > 0

    def flush_all(self):
        self._client.flushall()


def get_redis_client() -> RedisClient:
    redis_config = get_redis_config()

    return RedisClient(redis_config)
