import logging
import signal
import threading
from typing import Awaitable, Callable

from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.middleware.cors import CORSMiddleware

from app.config import Config
from app.bus import BusClient, BusMessage, get_bus_config
from app.router import api_router
from app.router.handlers.bus import get_bus_feedback_handler
from app.services.feedback import get_mail_mapping_feedback_service

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting Mail Mapper service...")

config = Config()

bus_topic_feedback = config.STREAM_FEEDBACK

bus_client: BusClient | None = None
feedback_handler = None
bus_retry_limit = 30
BUS_RETRY_DELAY_SECONDS = 1.0

if bus_topic_feedback:
    if not config.REDIS_URL:
        message = "REDIS_URL is not set"
        raise ValueError(message)
    if not config.STREAM_GROUP:
        message = "STREAM_GROUP is not set"
        raise ValueError(message)

    bus_config = get_bus_config()
    bus_retry_limit = bus_config.retries

    bus_client = BusClient(bus_config, config.STREAM_GROUP)
    feedback_service = get_mail_mapping_feedback_service()
    feedback_handler = get_bus_feedback_handler(feedback_service)
else:
    logger.warning(
        "STREAM_FEEDBACK is not configured; feedback consumer is disabled",
    )

_stop_event = threading.Event()
_consumer_thread: threading.Thread | None = None


def _log_feedback_message(message: BusMessage) -> None:
    logger.debug(
        "Consumed feedback message key=%s stream=%s id=%s value_type=%s",
        message.key,
        message.topic,
        message.entry_id,
        type(message.value).__name__,
    )


def _consume_feedback_messages() -> None:
    if bus_client is None or feedback_handler is None:
        logger.info("Feedback consumer loop skipped because the bus is not configured")

        return

    topic = bus_topic_feedback
    if not topic:
        logger.warning("Feedback topic is empty; consumer loop will not start")

        return

    bus_client.subscribe([topic])
    logger.info("Bus subscribed to feedback stream: %s", topic)

    while not _stop_event.is_set():
        message = bus_client.poll_once(timeout=1.0)
        if message is None:
            continue

        _log_feedback_message(message)

        try:
            if isinstance(message.value, dict):
                feedback_handler.handle(message.value)
            else:
                logger.error(
                    "Feedback message payload is not a dict: %s",
                    type(message.value).__name__,
                )
        except Exception:
            logger.exception("Feedback handler raised an exception")

            continue

        bus_client.commit(message)


def _feedback_consumer_loop() -> None:
    for attempt in range(1, bus_retry_limit + 1):
        if _stop_event.is_set():
            return

        try:
            logger.info(
                "Starting feedback consumer attempt %s/%s",
                attempt,
                bus_retry_limit,
            )
            _consume_feedback_messages()

            return
        except Exception as e:
            logger.exception(
                "Feedback consumer loop crashed on attempt %s/%s: %s",
                attempt,
                bus_retry_limit,
                e,
            )
        finally:
            logger.info("Feedback consumer loop stopped")

        if attempt == bus_retry_limit:
            logger.error(
                "Feedback consumer stopped after %s attempts",
                bus_retry_limit,
            )

            return

        if _stop_event.wait(BUS_RETRY_DELAY_SECONDS):
            return


def start_consumer_thread() -> None:
    if bus_client is None or feedback_handler is None:
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

    if bus_client is not None:
        bus_client.close()


def _handle_sigterm(*_: object) -> None:
    logger.info("Termination signal received, stopping feedback consumer...")
    stop_consumer_thread()


if bus_topic_feedback:
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
    if request.url.path == "/openapi.json":
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
