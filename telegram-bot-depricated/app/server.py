import asyncio
import json
import logging
import signal
from typing import Any, Callable, Optional, Tuple

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums.parse_mode import ParseMode
from confluent_kafka import Message

from app.kafka_client import KafkaClient, KafkaConfig
from app.redis_client import RedisClient, RedisConfig
from app.router.handlers.kafka import get_kafka_notification_handler
from app.router.router import router
from .config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting Telegram Bot...")

config = Config()

kafka_config = KafkaConfig(
    bootstrap_servers=config.KAFKA_SERVER,
    client_id=config.KAFKA_CLIENT_ID,
)
assert kafka_config.bootstrap_servers, "KAFKA_SERVER is not set"
logger.debug(
    "Kafka bootstrap: %s | client_id: %s",
    kafka_config.bootstrap_servers,
    kafka_config.client_id,
)
kafka_client = KafkaClient(kafka_config, config.KAFKA_CONSUMER_GROUP)

redis_client = RedisClient(RedisConfig())

kafka_notification_handler = get_kafka_notification_handler(
    redis_client,
)


def log_kafka_message(
    key: Any,
    value: Any,
    headers: dict[str, bytes],
    msg: Message,
) -> None:
    logger.info(
        "Consumed message: topic=%s key=%s value=%s partition=%s offset=%s",
        msg.topic(),
        key,
        value,
        msg.partition(),
        msg.offset(),
    )


async def _consumer_loop(bot: Bot, stop_event: asyncio.Event) -> None:
    topics = [
        topic
        for topic in (
            config.KAFKA_TOPIC_NOTIFICATION,
            config.KAFKA_TOPIC_NOTIFICATION_SYNC,
        )
        if topic
    ]

    if not topics:
        logger.error("No Kafka topics configured for Telegram Bot consumer")

        return

    try:
        await asyncio.to_thread(kafka_client.subscribe, topics)
        logger.info("Kafka subscribed to topics: %s", topics)

        md = await asyncio.to_thread(kafka_client.list_topics, 10.0)
        available_topics = list(md.topics.keys())

        for topic in topics:
            if topic not in md.topics:
                logger.error(
                    "Topic '%s' not found. Available topics: %s",
                    topic,
                    available_topics,
                )
            else:
                tmd = md.topics[topic]
                if tmd.error is not None:
                    logger.error("Topic '%s' metadata error: %s", topic, tmd.error)
                else:
                    parts = sorted(p.id for p in tmd.partitions.values())
                    logger.info("Topic '%s' partitions: %s", topic, parts)

        for _ in range(30):
            parts = await asyncio.to_thread(kafka_client.assignment)
            if parts:
                logger.info("Assigned partitions: %s", parts)
                break
            await asyncio.sleep(0.5)
        else:
            logger.warning("No partitions assigned within 15s")

        while not stop_event.is_set():
            item: Optional[Tuple[Any, Any, dict[str, bytes], Message]] = (
                await asyncio.to_thread(kafka_client.poll_once, 1.0)
            )
            if not item:
                continue

            key, value, headers, msg = item

            try:
                log_kafka_message(key, value, headers, msg)

                topic_name = msg.topic()

                await kafka_notification_handler.handle_notification(
                    bot,
                    topic_name,
                    key,
                    value,
                )
            except Exception:
                logger.exception("Handler error")
    except Exception:
        logger.exception("Consumer loop crashed")
    finally:
        await asyncio.to_thread(kafka_client.close_consumer)
        logger.info("Kafka consumer closed")


def to_text(value: Any) -> str:
    decoded = value
    if isinstance(decoded, (bytes, bytearray)):
        try:
            decoded = decoded.decode("utf-8")
        except Exception:
            decoded = decoded.decode("latin-1", errors="replace")

    if isinstance(decoded, dict):
        result = json.dumps(decoded, ensure_ascii=False, indent=2, sort_keys=True)

        return result

    result = str(decoded)

    return result


async def _run_bot() -> None:
    bot = Bot(
        token=config.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp = Dispatcher()

    # dp.message.middleware(UserContextMiddleware())
    dp.include_router(router)

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _on_stop_signal(*_: Any) -> None:
        logger.info("Shutdown signal received.")
        stop_event.set()

    def _add_signal_handler(sig: signal.Signals, handler: Callable[[], None]) -> None:
        try:
            loop.add_signal_handler(sig, handler)
        except NotImplementedError:
            pass

    for sig in (signal.SIGINT, signal.SIGTERM):
        _add_signal_handler(sig, _on_stop_signal)

    polling_task = asyncio.create_task(
        dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types()),
        name="aiogram-polling",
    )
    consumer_task = asyncio.create_task(
        _consumer_loop(bot, stop_event), name="_consumer_loop"
    )

    await stop_event.wait()

    polling_task.cancel()
    try:
        await polling_task
    except asyncio.CancelledError:
        pass

    try:
        await asyncio.wait_for(consumer_task, timeout=10)
    except asyncio.TimeoutError:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass

    await asyncio.to_thread(kafka_client.close_producer)


asyncio.run(_run_bot())

logger.info("Telegram Bot started!")
