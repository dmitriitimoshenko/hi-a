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

        update_request = self._build_update_request(context.payload)

        if update_request is None:
            await callback.answer("Nothing to apply", show_alert=True)
            self._logger.warning(
                "Update request is empty for event_id=%s",
                context.event_id,
            )

            return

        await callback.answer("Applying...")

        success = await self._push_update(update_request)

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

        update_request = self._build_external_update_request(context.payload)

        if update_request is None:
            await callback.answer("No DB snapshot is available", show_alert=True)
            self._logger.warning(
                "External update request is empty for event_id=%s",
                context.event_id,
            )

            return

        await callback.answer("Updating sheet...")

        success = await self._push_update_external(update_request)

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

    def _build_update_request(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        sheet_id = payload.get("sheet_id")
        sheet_payload = payload.get("sheet_payload") or {}

        if sheet_id is None or not sheet_payload:
            return None

        request: dict[str, Any] = {
            "sheet_id": sheet_id,
        }

        self._assign_if_present(request, "application_id", sheet_payload.get("application_id"))
        self._assign_if_present(request, "row_id", sheet_payload.get("row_id"))
        self._assign_if_present(request, "company", sheet_payload.get("company"))
        self._assign_if_present(request, "employment_type", sheet_payload.get("employment_type"))
        self._assign_if_present(request, "work_mode", sheet_payload.get("work_mode"))
        self._assign_if_present(request, "title", sheet_payload.get("title"))
        self._assign_if_present(request, "status", sheet_payload.get("status"))
        self._assign_if_present(
            request,
            "stage",
            sheet_payload.get("stage"),
            allow_null=True,
        )
        self._assign_if_present(request, "applied_at", sheet_payload.get("applied_at"))
        self._assign_if_present(request, "responded_at", sheet_payload.get("responded_at"), allow_null=True)
        self._assign_if_present(request, "next_follow_up_at", sheet_payload.get("next_follow_up_at"), allow_null=True)

        meta = sheet_payload.get("meta")
        if isinstance(meta, dict):
            request["meta"] = meta

        salary_applied = sheet_payload.get("salary_applied")
        salary_proposed = sheet_payload.get("salary_proposed")

        if isinstance(salary_applied, dict):
            self._assign_if_present(request, "salary_applied_from", salary_applied.get("amount_from"))
            self._assign_if_present(request, "salary_applied_to", salary_applied.get("amount_to"))
            self._assign_if_present(request, "salary_currency", salary_applied.get("currency"))
            self._assign_if_present(request, "salary_period", salary_applied.get("period"))

        if isinstance(salary_proposed, dict):
            self._assign_if_present(request, "salary_offered_from", salary_proposed.get("amount_from"))
            self._assign_if_present(request, "salary_offered_to", salary_proposed.get("amount_to"))
            if "salary_currency" not in request:
                self._assign_if_present(request, "salary_currency", salary_proposed.get("currency"))
            if "salary_period" not in request:
                self._assign_if_present(request, "salary_period", salary_proposed.get("period"))

        return request

    def _build_external_update_request(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        sheet_id = payload.get("sheet_id")
        sheet_page = payload.get("sheet_page") or "applications_list"
        db_snapshot = payload.get("db_snapshot")
        row_id = (db_snapshot or {}).get("row_id")

        if sheet_id is None or not isinstance(db_snapshot, dict) or row_id is None:
            return None

        request: dict[str, Any] = {
            "sheet_id": sheet_id,
            "sheet_page": sheet_page,
            "db_snapshot": db_snapshot,
        }

        return request

    async def _push_update(self, request: dict[str, Any]) -> bool:
        base_url = self._config.GSA_BASE_URL.rstrip("/")
        url = f"{base_url}/api/application/update-internal"
        headers = {**HEADERS, "x-api-version": self._config.API_VERSION}

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
                response = await client.post(url, headers=headers, json=request)
        except Exception as e:
            self._logger.error("Failed to call application update endpoint: %s", e)

            return False

        if response.status_code >= 300:
            self._logger.error(
                "Application update request failed status=%s body=%s",
                response.status_code,
                response.text,
            )

            return False

        self._logger.info(
            "Synchronized application via GSA update endpoint (application_id=%s row_id=%s)",
            request.get("application_id"),
            request.get("row_id"),
        )

        return True

    async def _push_update_external(self, request: dict[str, Any]) -> bool:
        base_url = self._config.GSA_BASE_URL.rstrip("/")
        url = f"{base_url}/api/application/update-external"
        headers = {**HEADERS, "x-api-version": self._config.API_VERSION}

        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
                response = await client.post(url, headers=headers, json=request)
        except Exception as e:
            self._logger.error(
                "Failed to call application update-external endpoint: %s",
                e,
            )

            return False

        if response.status_code >= 300:
            self._logger.error(
                "Application update-external request failed status=%s body=%s",
                response.status_code,
                response.text,
            )

            return False

        self._logger.info(
            "Synchronized Google Sheet via update-external endpoint (row_id=%s)",
            (request.get("db_snapshot") or {}).get("row_id"),
        )

        return True

    def _assign_if_present(
        self,
        target: dict[str, Any],
        key: str,
        value: Any,
        *,
        allow_null: bool = False,
    ) -> None:
        if value is None and not allow_null:
            return

        target[key] = value

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
