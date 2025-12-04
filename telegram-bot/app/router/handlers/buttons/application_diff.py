import json
import logging
from dataclasses import dataclass
from typing import Any

import httpx
from aiogram.types import CallbackQuery, Message

from app.config import Config
from app.redis_client.client import RedisClient, get_redis_client
from .message_editor import append_button_response


APPLICATION_DIFF_CALLBACK_PREFIX = "application_diff"
APPLICATION_DIFF_REDIS_PREFIX = "application_diff"
HEADERS = {
    "content-type": "application/json",
}
DIFF_UPDATE_ENDPOINT = "/api/application/diff/update"
UPDATE_DIRECTION_INTERNAL = "internal"
UPDATE_DIRECTION_EXTERNAL = "external"


@dataclass
class CachedDiffContext:
    message: Message
    event_id: str
    redis_key: str
    payload: dict[str, Any]


class ApplicationDiffButtonHandler:
    def __init__(
        self,
        *,
        redis_client: RedisClient,
        config: Config,
    ) -> None:
        self._redis_client = redis_client
        self._config = config
        self._logger = logging.getLogger(__name__)

    async def apply_sheet(self, callback: CallbackQuery) -> None:
        context = await self._load_context(callback)

        if context is None:
            return

        update_request = self._build_diff_update_request(
            payload=context.payload,
            update_direction=UPDATE_DIRECTION_INTERNAL,
            event_id=context.event_id,
        )

        if update_request is None:
            await callback.answer("Application data is unavailable", show_alert=True)
            self._logger.warning(
                "Diff update request is empty for event_id=%s direction=%s",
                context.event_id,
                UPDATE_DIRECTION_INTERNAL,
            )

            return

        await callback.answer("Applying...")

        success = await self._push_diff_update(update_request)

        if not success:
            await context.message.answer("⭕ Failed to apply changes, please retry later")

            return

        await self._cleanup_context(context)

        updated = await append_button_response(
            message=context.message,
            response_text="✅ Changes applied to the database",
            logger=self._logger,
        )
        if not updated:
            await context.message.answer("✅ Changes applied to the database")

    async def apply_internal(self, callback: CallbackQuery) -> None:
        context = await self._load_context(callback)

        if context is None:
            return

        update_request = self._build_diff_update_request(
            payload=context.payload,
            update_direction=UPDATE_DIRECTION_EXTERNAL,
            event_id=context.event_id,
        )

        if update_request is None:
            await callback.answer("Application data is unavailable", show_alert=True)
            self._logger.warning(
                "Diff update request is empty for event_id=%s direction=%s",
                context.event_id,
                UPDATE_DIRECTION_EXTERNAL,
            )

            return

        await callback.answer("Updating sheet...")

        success = await self._push_diff_update(update_request)

        if not success:
            await context.message.answer("⭕ Failed to sync Google Sheet, please retry later")

            return

        await self._cleanup_context(context)

        updated = await append_button_response(
            message=context.message,
            response_text="✅ Google Sheet updated from the database",
            logger=self._logger,
        )
        if not updated:
            await context.message.answer("✅ Google Sheet updated from the database")

    async def skp(self, callback: CallbackQuery) -> None:
        message = callback.message if isinstance(callback.message, Message) else None
        event_id = self._extract_event_id(callback.data or "")

        if event_id is not None:
            redis_key = self._build_redis_key(event_id)
            self._redis_client.delete(redis_key)

        await callback.answer("Skipped")

        if message is not None:
            updated = await append_button_response(
                message=message,
                response_text="Skipped",
                logger=self._logger,
            )
            if not updated:
                try:
                    await message.edit_reply_markup()
                except Exception:
                    self._logger.debug(
                        "Failed to clear inline keyboard after skip for event_id=%s",
                        event_id,
                    )

    def _extract_event_id(self, callback_data: str) -> str | None:
        parts = callback_data.split(":")

        if len(parts) != 3:
            return None

        prefix, _action, event_id = parts
        if prefix != APPLICATION_DIFF_CALLBACK_PREFIX or not event_id:
            return None

        return event_id

    def _build_diff_update_request(
        self,
        *,
        payload: dict[str, Any],
        update_direction: str,
        event_id: str,
    ) -> dict[str, Any] | None:
        application_id = payload.get("application_id")

        if application_id is None:
            self._logger.warning(
                "application_id is missing in diff payload for event_id=%s",
                event_id,
            )

            return None

        try:
            normalized_application_id = int(application_id)
        except Exception as e:
            self._logger.error(
                "application_id is not an integer (event_id=%s, application_id=%s): %s",
                event_id,
                application_id,
                e,
            )

            return None

        request = {
            "application_id": normalized_application_id,
            "update_direction": update_direction,
        }

        return request

    async def _push_diff_update(self, request: dict[str, Any]) -> bool:
        base_url = self._config.GSA_BASE_URL.rstrip("/")
        url = f"{base_url}{DIFF_UPDATE_ENDPOINT}"
        headers = {**HEADERS, "x-api-version": self._config.API_VERSION}

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
                response = await client.post(url, headers=headers, json=request)
        except Exception as e:
            self._logger.error(
                "Failed to call diff update endpoint: %s",
                e,
            )

            return False

        if response.status_code >= 300:
            self._logger.error(
                "Diff update request failed status=%s body=%s",
                response.status_code,
                response.text,
            )

            return False

        self._logger.info(
            "Triggered diff update via GSA endpoint (application_id=%s direction=%s)",
            request.get("application_id"),
            request.get("update_direction"),
        )

        return True

    async def _load_context(self, callback: CallbackQuery) -> CachedDiffContext | None:
        message = callback.message

        if not isinstance(message, Message):
            await callback.answer("Message is unavailable", show_alert=True)
            self._logger.error("application_diff callback has no message attached")

            return None

        event_id = self._extract_event_id(callback.data or "")

        if event_id is None:
            await callback.answer("Invalid callback payload", show_alert=True)
            self._logger.error("Failed to parse event id from callback: %s", callback.data)

            return None

        redis_key = self._build_redis_key(event_id)
        cached_payload = self._redis_client.get(redis_key)

        if cached_payload is None:
            await callback.answer("Changes data expired", show_alert=True)
            self._logger.error("Cached payload is missing for event_id=%s", event_id)

            return None

        try:
            payload = json.loads(cached_payload)
        except Exception as e:
            await callback.answer("Changes data corrupted", show_alert=True)
            self._logger.error(
                "Failed to decode cached payload for event_id=%s: %s",
                event_id,
                e,
            )

            return None

        context = CachedDiffContext(
            message=message,
            event_id=event_id,
            redis_key=redis_key,
            payload=payload,
        )

        return context

    async def _cleanup_context(self, context: CachedDiffContext) -> None:
        self._redis_client.delete(context.redis_key)

        try:
            await context.message.edit_reply_markup()
        except Exception:
            self._logger.debug(
                "Failed to clear inline keyboard for event_id=%s",
                context.event_id,
            )

    def _build_redis_key(self, event_id: str) -> str:
        key = f"{APPLICATION_DIFF_REDIS_PREFIX}:{event_id}"

        return key


def get_application_diff_button_handler() -> ApplicationDiffButtonHandler:
    redis_client = get_redis_client()
    config = Config()

    handler = ApplicationDiffButtonHandler(
        redis_client=redis_client,
        config=config,
    )

    return handler
