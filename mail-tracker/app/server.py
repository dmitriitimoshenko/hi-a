import logging
from typing import Awaitable, Callable

from fastapi import FastAPI, Request, Response, status
from fastapi.routing import APIRoute
from fastapi.responses import JSONResponse
from starlette.middleware.cors import CORSMiddleware

from app.config import Config
from app.integrations.gmail.watcher import (
    GmailImapIdleWatcher,
    ImapCredentials,
    ImapWatchConfig,
)
from app.kafka_client import KafkaClient, KafkaConfig
from app.redis_client import RedisConfig, RedisClient
from app.router.handlers.new_mail_handler import NewMailHandler
from app.router import api_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info("Starting Mail Tracker service...")

config = Config()

kafka_config = KafkaConfig(
    bootstrap_servers=config.KAFKA_SERVER or "",
    client_id=config.KAFKA_CLIENT_ID or "",
)
kafka_client = KafkaClient(kafka_config)

redis_client = RedisClient(RedisConfig())

new_mail_handler = NewMailHandler(
    kafka_client,
    topic=config.KAFKA_TOPIC_NEW_MAIL,
)
gmail_user = config.GMAIL_USER
gmail_app_password = config.GMAIL_APP_PASSWORD
creds = ImapCredentials(
    user=gmail_user or "",
    app_password=gmail_app_password or "",
)
watcher = GmailImapIdleWatcher(
    redis_client,
    creds,
    ImapWatchConfig(folder="INBOX"),
    on_message=new_mail_handler.handle,
)
watcher.start()


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

logger.info("Mail Tracker service started!")
