import json
import logging
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from confluent_kafka import Message
from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from fastapi.routing import APIRoute
from starlette.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app.integrations.open_ai.client import OpenAIClient, OpenAIConfig
from app.integrations.google_sheets_client.service.service import get_sheets_service
from app.integrations.open_ai.service import OpenAIService
from app.redis_client import RedisConfig, RedisClient
from app.router.router import api_router
from app.kafka_client import KafkaConsumer
from app.kafka_client.config import get_kafka_config
from app.database.database import get_session_maker
from app.models.application import Application
from app.service.application.service import get_application_service
from .config import Config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Starting Google Sheets Accessor service...")

config = Config()

MEETING_CONFIRMATION_LABELS = {"meeting_crt", "meeting_inv"}
MEETING_CREATION_LABEL = "meeting_crt"

google_sheets_service = get_sheets_service()

openai_config = OpenAIConfig(api_key=config.OPENAI_API_KEY or "")
openai_client = OpenAIClient(openai_config)
openai_service = OpenAIService(openai_client)

redis_client = RedisClient(RedisConfig())

session_maker = get_session_maker()

try:
    kafka_config = get_kafka_config()
except ValueError as err:
    kafka_config = None
    logger.warning("Kafka configuration missing: %s", err)

_consumer_stop_event = threading.Event()
_consumer_thread: threading.Thread | None = None
_consumer_topics: list[str] = []
kafka_consumer: KafkaConsumer | None = None

if kafka_config:
    _consumer_topics = [
        topic
        for topic in (
            config.KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING,
            config.KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED,
        )
        if topic
    ]

if kafka_config and _consumer_topics:
    consumer_group = config.KAFKA_CONSUMER_GROUP or "google-sheets-accessor"
    try:
        kafka_consumer = KafkaConsumer(
            kafka_config,
            consumer_group,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
    except Exception as exc:
        kafka_consumer = None
        logger.exception("Failed to initialize Kafka consumer: %s", exc)
else:
    kafka_consumer = None


def _process_embedding_message(msg: Message) -> None:
    key_bytes = msg.key() or b""
    key_str = key_bytes.decode("utf-8", errors="ignore").strip()

    if not key_str:
        logger.warning("Received embedding message without key")
        return

    try:
        application_id = int(key_str)
    except ValueError:
        logger.warning("Invalid application id in embedding message: %s", key_str)
        return

    value_bytes = msg.value() or b""
    try:
        payload = json.loads(value_bytes.decode("utf-8"))
    except json.JSONDecodeError:
        logger.warning(
            "Failed to decode embedding payload for application %s", application_id
        )
        return

    if not isinstance(payload, list):
        logger.warning(
            "Embedding payload for application %s is not a list", application_id
        )
        return

    try:
        embedding = [float(item) for item in payload]
    except (TypeError, ValueError):
        logger.warning(
            "Embedding payload for application %s contains non-numeric values",
            application_id,
        )
        return

    session: Session = session_maker()
    try:
        application = session.get(Application, application_id)
        if application is None:
            logger.warning(
                "Application %s not found when saving embedding", application_id
            )
            return

        application.embedding = embedding
        session.add(application)
        session.commit()
        logger.info("Embedding saved for application %s", application_id)
    except Exception:
        session.rollback()
        logger.exception(
            "Failed to save embedding for application %s", application_id
        )
    finally:
        session.close()


def _process_application_update_message(msg: Message) -> None:
    key_bytes = msg.key() or b""
    key_str = key_bytes.decode("utf-8", errors="ignore").strip()

    value_bytes = msg.value() or b""
    try:
        payload = json.loads(value_bytes.decode("utf-8"))
    except json.JSONDecodeError:
        logger.warning(
            "Failed to decode application update payload for key %s",
            key_str or "<empty>",
        )
        return

    logger.info(
        "Received application update message key=%s payload=%s",
        key_str or "<empty>",
        payload,
    )

    try:
        _apply_application_update(payload)
    except Exception:
        logger.exception(
            "Failed to apply application update for key=%s payload=%s",
            key_str or "<empty>",
            payload,
        )


def _apply_application_update(payload: Any) -> None:
    if not isinstance(payload, dict):
        logger.warning("application update payload is not a dict: %s", type(payload))

        return

    is_meeting_confirmation = _is_meeting_confirmation(payload)
    is_meeting_creation_confirmation = _is_meeting_creation_confirmation(payload)
    is_denied_confirmation = _is_denied_confirmation(payload)

    if not (is_meeting_confirmation or is_denied_confirmation):
        return

    sheet_id = config.SHEET_ID
    if not sheet_id:
        logger.warning("Skipping application update: SHEET_ID is not configured")

        return

    mapped_application = payload.get("mapped_application") or {}
    if not isinstance(mapped_application, dict):
        logger.warning(
            "Skipping application update: mapped_application is missing or invalid",
        )

        return

    application_id = mapped_application.get("id")
    row_id = mapped_application.get("row_id")

    if application_id is None or row_id is None:
        logger.warning(
            "Skipping application update: application_id or row_id is missing "
            "(application_id=%s, row_id=%s)",
            application_id,
            row_id,
        )

        return

    stage_value: int | None = None
    if is_meeting_creation_confirmation:
        stage_raw = mapped_application.get("stage")
        stage_value = _parse_stage(stage_raw)

    status_value = "denied" if is_denied_confirmation else payload.get("status") or mapped_application.get("status")
    responded_at_value = payload.get("responded_at") or mapped_application.get("responded_at")
    if is_denied_confirmation and not responded_at_value:
        responded_at_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    next_follow_up_value = None
    if is_meeting_creation_confirmation:
        next_follow_up_value = payload.get("next_follow_up_at") or mapped_application.get("next_follow_up_at")

    session: Session = session_maker()
    try:
        application_service = get_application_service(db_session=session)
        update_payload = {
            "application_id": application_id,
            "row_id": row_id,
        }
        if stage_value is not None:
            update_payload["stage"] = stage_value
        if status_value is not None:
            update_payload["status"] = status_value
        if responded_at_value is not None:
            update_payload["responded_at"] = responded_at_value
        if next_follow_up_value is not None:
            update_payload["next_follow_up_at"] = next_follow_up_value
        logger.info(
            "Applying application update: app_id=%s row_id=%s payload=%s",
            application_id,
            row_id,
            update_payload,
        )

        application_service.update(update_payload)

        if is_meeting_creation_confirmation:
            logger.info(
                "Stage updated for %s: application_id=%s row_id=%s stage=%s",
                _get_email_label(payload) or "meeting_confirmation",
                application_id,
                row_id,
                stage_value,
            )
        elif is_denied_confirmation:
            logger.info(
                "Application marked as denied: application_id=%s row_id=%s responded_at=%s",
                application_id,
                row_id,
                responded_at_value,
            )
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _is_meeting_creation_confirmation(payload: dict[str, Any]) -> bool:
    label = _get_email_label(payload)
    action = str(payload.get("action") or "").lower()

    if label != MEETING_CREATION_LABEL:
        return False

    if action and action != "cnfm":
        return False

    return True


def _is_meeting_confirmation(payload: dict[str, Any]) -> bool:
    label = _get_email_label(payload)
    action = str(payload.get("action") or "").lower()

    if label not in MEETING_CONFIRMATION_LABELS:
        return False

    if action and action != "cnfm":
        return False

    return True


def _is_denied_confirmation(payload: dict[str, Any]) -> bool:
    email = payload.get("email") or {}
    label = str(email.get("label") or "").lower()
    action = str(payload.get("action") or "").lower()

    if label != "denied":
        return False

    if action and action != "cnfm":
        return False

    return True


def _parse_stage(value: Any) -> int:
    if value is None:
        return 0

    try:
        return int(str(value).strip())
    except Exception:
        logger.warning("Invalid stage value '%s', defaulting to 0", value)

        return 0


def _get_email_label(payload: dict[str, Any]) -> str:
    email = payload.get("email") or {}

    return str(email.get("label") or "").lower()


def _consumer_loop() -> None:
    if kafka_consumer is None:
        logger.info("Kafka consumer not initialized; skipping consumption")
        return

    topics = list(_consumer_topics)
    if not topics:
        logger.info("No Kafka topics configured; skipping consumer thread")
        return

    try:
        kafka_consumer.subscribe(topics)
        logger.info("Kafka subscribed to topics: %s", topics)

        metadata = kafka_consumer.list_topics(timeout=10.0)
        for topic in topics:
            if topic not in metadata.topics:
                logger.error(
                    "Topic '%s' not found. Available topics: %s",
                    topic,
                    list(metadata.topics.keys()),
                )
                continue

            topic_metadata = metadata.topics[topic]
            if topic_metadata.error is not None:
                logger.error("Topic '%s' metadata error: %s", topic, topic_metadata.error)
                continue

            partitions = sorted(p.id for p in topic_metadata.partitions.values())
            logger.info("Topic '%s' partitions: %s", topic, partitions)

        for _ in range(30):
            partitions = kafka_consumer.assignment()
            if partitions:
                logger.info("Assigned partitions: %s", partitions)
                break
            time.sleep(0.5)
        else:
            logger.warning("No partitions assigned within 15s")

        while not _consumer_stop_event.is_set():
            msg = kafka_consumer.poll(timeout=1.0)
            if msg is None:
                continue
            try:
                topic = msg.topic()

                if topic == config.KAFKA_TOPIC_SAVE_APPLICATION_EMBEDDING:
                    _process_embedding_message(msg)
                elif topic == config.KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED:
                    _process_application_update_message(msg)
                else:
                    logger.warning("No handler configured for topic '%s'", topic)

                kafka_consumer.commit(msg)
            except Exception:
                logger.exception("Error while handling kafka message")
    except Exception:
        logger.exception("Kafka consumer thread crashed")
    finally:
        if kafka_consumer is not None:
            kafka_consumer.close()
            logger.info("Kafka consumer closed")


def _start_consumer_thread() -> None:
    global _consumer_thread
    if kafka_consumer is None:
        return

    if _consumer_thread and _consumer_thread.is_alive():
        return

    _consumer_stop_event.clear()
    _consumer_thread = threading.Thread(
        target=_consumer_loop,
        name="kafka-consumer",
        daemon=True,
    )
    _consumer_thread.start()


def _stop_consumer_thread() -> None:
    if kafka_consumer is None:
        return

    _consumer_stop_event.set()
    if _consumer_thread:
        _consumer_thread.join(timeout=10)


def _handle_sigterm(*_: object) -> None:
    logger.info("Shutdown signal received, stopping kafka consumer")
    _stop_consumer_thread()


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

if kafka_consumer is not None:
    _start_consumer_thread()


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

logger.info("Google Sheets Accessor service started!")
