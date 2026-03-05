import logging
import signal
import threading
import time
from typing import Any, Awaitable, Callable

from confluent_kafka import Message

from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.middleware.cors import CORSMiddleware

from app.config import Config
from app.kafka_client import KafkaClient, KafkaConfig
from app.router import api_router
from app.router.handlers.kafka import get_kafka_feedback_handler
from app.services.feedback import get_mail_mapping_feedback_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting Mail Mapper service...")

config = Config()

kafka_topic_feedback = config.KAFKA_TOPIC_FEEDBACK

kafka_client: KafkaClient | None = None
feedback_handler = None
kafka_retry_limit = 30
KAFKA_RETRY_DELAY_SECONDS = 1.0

if kafka_topic_feedback:
    if not config.KAFKA_SERVER:
        message = "KAFKA_SERVER is not set"
        raise ValueError(message)
    if not config.KAFKA_CLIENT_ID:
        message = "KAFKA_CLIENT_ID is not set"
        raise ValueError(message)
    if not config.KAFKA_CONSUMER_GROUP:
        message = "KAFKA_CONSUMER_GROUP is not set"
        raise ValueError(message)

    kafka_config = KafkaConfig(
        bootstrap_servers=config.KAFKA_SERVER,
        client_id=config.KAFKA_CLIENT_ID,
        group_id=config.KAFKA_CONSUMER_GROUP,
    )
    kafka_retry_limit = kafka_config.kafka_retries

    kafka_client = KafkaClient(kafka_config)
    feedback_service = get_mail_mapping_feedback_service()
    feedback_handler = get_kafka_feedback_handler(feedback_service)
else:
    logger.warning(
        "KAFKA_TOPIC_FEEDBACK is not configured; feedback consumer is disabled",
    )

_stop_event = threading.Event()
_consumer_thread: threading.Thread | None = None


def _log_feedback_message(
    key: Any,
    value: Any,
    msg: Message,
) -> None:
    logger.debug(
        "Consumed feedback message key=%s partition=%s offset=%s value_type=%s",
        key,
        msg.partition(),
        msg.offset(),
        type(value).__name__,
    )


def _consume_feedback_messages() -> None:
    if kafka_client is None or feedback_handler is None:
        logger.info("Feedback consumer loop skipped because Kafka is not configured")

        return

    topic = kafka_topic_feedback
    if not topic:
        logger.warning("Feedback topic is empty; consumer loop will not start")

        return

    kafka_client.subscribe([topic])
    logger.info("Kafka subscribed to feedback topic: %s", topic)

    metadata = kafka_client.list_topics(timeout=10.0)
    if topic not in metadata.topics:
        logger.error(
            "Topic '%s' not found. Available topics: %s",
            topic,
            list(metadata.topics.keys()),
        )
    else:
        topic_metadata = metadata.topics[topic]
        if topic_metadata.error is not None:
            logger.error(
                "Topic '%s' metadata error: %s",
                topic,
                topic_metadata.error,
            )
        else:
            partitions = sorted(p.id for p in topic_metadata.partitions.values())
            logger.info("Topic '%s' partitions: %s", topic, partitions)

    while not _stop_event.is_set():
        item = kafka_client.poll_once(timeout=1.0)
        if not item:
            continue

        key, value, _headers, msg = item
        _log_feedback_message(key, value, msg)

        try:
            if isinstance(value, dict):
                feedback_handler.handle(value)
            else:
                logger.error(
                    "Feedback message payload is not a dict: %s",
                    type(value).__name__,
                )
        except Exception:
            logger.exception("Feedback handler raised an exception")


def _feedback_consumer_loop() -> None:
    for attempt in range(1, kafka_retry_limit + 1):
        if _stop_event.is_set():
            return

        try:
            logger.info(
                "Starting feedback consumer attempt %s/%s",
                attempt,
                kafka_retry_limit,
            )
            _consume_feedback_messages()

            return
        except Exception as e:
            logger.exception(
                "Feedback consumer loop crashed on attempt %s/%s: %s",
                attempt,
                kafka_retry_limit,
                e,
            )
        finally:
            if kafka_client is not None:
                kafka_client.close_consumer()

            logger.info("Feedback consumer loop stopped")

        if attempt == kafka_retry_limit:
            logger.error(
                "Feedback consumer stopped after %s attempts",
                kafka_retry_limit,
            )

            return

        if _stop_event.wait(KAFKA_RETRY_DELAY_SECONDS):
            return


def start_consumer_thread() -> None:
    if kafka_client is None or feedback_handler is None:
        return

    global _consumer_thread
    if _consumer_thread and _consumer_thread.is_alive():
        return

    _stop_event.clear()
    _consumer_thread = threading.Thread(
        target=_feedback_consumer_loop,
        name="feedback-consumer",
        daemon=True,
    )
    _consumer_thread.start()


def stop_consumer_thread() -> None:
    _stop_event.set()
    if _consumer_thread and _consumer_thread.is_alive():
        _consumer_thread.join(timeout=10)

    if kafka_client is not None:
        kafka_client.close_consumer()
        kafka_client.close_producer()


def _handle_sigterm(*_: object) -> None:
    logger.info("Termination signal received, stopping feedback consumer...")
    stop_consumer_thread()


if kafka_topic_feedback:
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    start_consumer_thread()


def custom_generate_unique_id(route: APIRoute) -> str:
    unique_id = f"{route.tags[0]}-{route.name}"

    return unique_id


app = FastAPI(
    title=config.SERVICE_NAME,
    generate_unique_id_function=custom_generate_unique_id,
    openapi_url="/openapi.json",
)
app.add_middleware(CORSMiddleware)


@app.middleware("http")
async def enforce_api_version(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    if request.url.path in {"/openapi.json", "/api/metrics"}:
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

logger.info("Mail Mapper service started!")
