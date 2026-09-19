import logging
import signal
import threading
from typing import Awaitable, Callable

from fastapi import FastAPI, Request, Response, status
from fastapi.routing import APIRoute
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware

from app.bus import BusClient, BusMessage, get_bus_config
from app.router import api_router
from app.router.handlers.bus import get_bus_event_handler
from app.config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting Mail Processor service...")

config = Config()

bus_config = get_bus_config()
assert bus_config.redis_url, "REDIS_URL is not set"
logger.debug(
    "Bus redis: %s | consumer: %s",
    bus_config.redis_url,
    bus_config.consumer_id,
)
bus_client = BusClient(bus_config, config.STREAM_GROUP)

bus_event_handler = get_bus_event_handler(config)

_stop_event = threading.Event()
_consumer_thread = None
BUS_RETRY_DELAY_SECONDS = 1.0


def log_bus_message(message: BusMessage) -> None:
    logger.info(
        "Consumed message: stream=%s id=%s key=%s value=%s",
        message.topic,
        message.entry_id,
        message.key,
        message.value,
    )


def _consume_messages() -> None:
    topic = config.STREAM_NEW_MAIL
    bus_client.subscribe([topic])
    logger.info("Bus subscribed to streams: %s", [topic])

    while not _stop_event.is_set():
        message = bus_client.poll_once(timeout=1.0)
        if message is None:
            continue

        try:
            log_bus_message(message)

            bus_event_handler.handle(message.key, message.value)
        except Exception:
            logger.exception("Handler error")

            continue

        bus_client.commit(message)


def _consumer_loop() -> None:
    for attempt in range(1, bus_config.retries + 1):
        if _stop_event.is_set():
            return

        try:
            logger.info(
                "Starting bus consumer attempt %s/%s",
                attempt,
                bus_config.retries,
            )
            _consume_messages()

            return
        except Exception as e:
            logger.exception(
                "Consumer thread crashed on attempt %s/%s: %s",
                attempt,
                bus_config.retries,
                e,
            )

        if attempt == bus_config.retries:
            logger.error(
                "Bus consumer stopped after %s attempts",
                bus_config.retries,
            )

            return

        if _stop_event.wait(BUS_RETRY_DELAY_SECONDS):
            return


def start_consumer_thread() -> None:
    global _consumer_thread
    if _consumer_thread and _consumer_thread.is_alive():
        return
    _consumer_thread = threading.Thread(
        target=_consumer_loop, name="bus-consumer", daemon=True
    )
    _consumer_thread.start()


def stop_consumer_thread() -> None:
    _stop_event.set()
    if _consumer_thread:
        _consumer_thread.join(timeout=10)
    bus_client.close()


def _handle_sigterm(*_) -> None:
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

logger.info("Mail Processor service started!")
