import logging
import signal
import threading
import time
from typing import Any, Awaitable, Callable

from confluent_kafka import Message
from fastapi import FastAPI, Request, Response, status
from fastapi.routing import APIRoute
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware

from app.kafka_client import KafkaClient, KafkaConfig
from app.router import api_router
from app.router.handlers.kafka import get_kafka_event_handler
from app.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting Hire Event Processor service...")

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

kafka_event_handler = get_kafka_event_handler(config, kafka_client)

_stop_event = threading.Event()
_consumer_thread = None


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


def _consumer_loop() -> None:
    topics = [
        topic
        for topic in (
            config.KAFKA_TOPIC_INTERESTING_MAIL,
            config.KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED,
            config.KAFKA_TOPIC_APPLICATIONS_SYNC_UNPROCESSED,
        )
        if topic
    ]

    if not topics:
        logger.error("No Kafka topics configured for Hire Event Processor consumer")

        return

    try:
        kafka_client.subscribe(topics)
        logger.info("Kafka subscribed to topics: %s", topics)

        md = kafka_client.list_topics(timeout=10.0)
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
            parts = kafka_client.assignment()
            if parts:
                logger.info("Assigned partitions: %s", parts)
                break
            time.sleep(0.5)
        else:
            logger.warning("No partitions assigned within 15s")

        while not _stop_event.is_set():
            item = kafka_client.poll_once(timeout=1.0)
            if not item:
                continue
            key, value, headers, msg = item
            try:
                log_kafka_message(key, value, headers, msg)

                topic = msg.topic()
                if topic == config.KAFKA_TOPIC_INTERESTING_MAIL:
                    kafka_event_handler.handle_interesting_mail(key, value)
                elif topic == config.KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED:
                    kafka_event_handler.handle_application_update(key, value)
                elif topic == config.KAFKA_TOPIC_APPLICATIONS_SYNC_UNPROCESSED:
                    kafka_event_handler.handle_application_sync(key, value)
                else:
                    logger.warning("No handler configured for topic %s", topic)
            except Exception:
                logger.exception("Handler error")
    except Exception:
        logger.exception("Consumer thread crashed")
    finally:
        kafka_client.close_consumer()
        logger.info("Kafka consumer closed")


def start_consumer_thread() -> None:
    global _consumer_thread
    if _consumer_thread and _consumer_thread.is_alive():
        return
    _consumer_thread = threading.Thread(
        target=_consumer_loop, name="kafka-consumer", daemon=True
    )
    _consumer_thread.start()


def stop_consumer_thread() -> None:
    _stop_event.set()
    if _consumer_thread:
        _consumer_thread.join(timeout=10)
    kafka_client.close_producer()


def _handle_sigterm(*_: Any) -> None:
    logger.info("SIGTERM received, stopping...")
    stop_consumer_thread()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

start_consumer_thread()

# --------- API ---------


def custom_generate_unique_id(route: APIRoute) -> str:
    unique_id = f"{route.tags[0]}-{route.name}"

    return unique_id


app = FastAPI(
    title=config.SERVICE_NAME,
    generate_unique_id_function=custom_generate_unique_id,
    openapi_url=f"/openapi.json",
)

app.add_middleware(CORSMiddleware)


@app.middleware("http")
async def check_api_version(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    if request.url.path in ["/openapi.json"]:
        response = await call_next(request)

        return response

    expected_version = config.API_VERSION
    api_version = request.headers.get("X-API-Version")

    if api_version != expected_version:
        error_response = JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "Invalid or missing API version"},
        )

        return error_response

    response = await call_next(request)

    return response


app.include_router(api_router, prefix="/api")

logger.info("Hire Event Processor service started!")
