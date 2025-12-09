from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable

from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from app.kafka_client.client import KafkaClient, get_kafka_client
from app.redis_client.client import RedisClient, get_redis_client
from .message_editor import append_button_response

logging.basicConfig(level=logging.INFO)

CONFIRMATION_MESSAGE = "✅ Confirmed"
ERROR_MESSAGE = "⭕ INTERNAL ERROR OCCURED ⭕"
DETAILS_PENDING_MESSAGE = "☑️ Details will follow..."
DETAILS_MISSING_MESSAGE = "Details for this event are not available anymore 🫥"
SKIP_PROMPT_MESSAGE = "Why skipping?"
SKIP_ACTION = "skp"
SKIP_REASON_EMAIL_MISCLASSIFIED = "email_misclassified"
SKIP_REASON_APPLICATION_MISMATCH = "application_mismatch"
SKIP_REASON_OTHER = "other_reason"
SKIP_REASON_BACK = "back"
SKIP_REASON_ORDER = [
    SKIP_REASON_EMAIL_MISCLASSIFIED,
    SKIP_REASON_APPLICATION_MISMATCH,
    SKIP_REASON_OTHER,
    SKIP_REASON_BACK,
]
SKIP_REASON_TEXTS = {
    SKIP_REASON_EMAIL_MISCLASSIFIED: "Email misclassified",
    SKIP_REASON_APPLICATION_MISMATCH: "Application mismatch",
    SKIP_REASON_OTHER: "Other reason",
    SKIP_REASON_BACK: "Back",
}
KAFKA_TOPIC_ENV = "KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED"
FEEDBACK_TOPIC_ENV = "KAFKA_TOPIC_FEEDBACK"
MAX_MESSAGE_CHARS = 4000


class BaseButtonHandler:
    def __init__(
        self,
        *,
        kafka_client: KafkaClient,
        redis_client: RedisClient,
        logger_name: str,
    ) -> None:
        self._kafka_client = kafka_client
        self._redis_client = redis_client
        self._logger = logging.getLogger(logger_name)

    async def cnfm(self, callback: CallbackQuery) -> None:
        message = self._require_message(callback)
        if message is None:
            return

        success = await self._publish_application_update(callback, message)
        if success:
            target_message = self._resolve_root_message(message)
            updated = await append_button_response(
                message=target_message,
                response_text=CONFIRMATION_MESSAGE,
                logger=self._logger,
            )
            if not updated:
                await target_message.answer(CONFIRMATION_MESSAGE)

            return

        await message.answer(ERROR_MESSAGE)

    async def skp(self, callback: CallbackQuery) -> None:
        message = self._require_message(callback)
        if message is None:
            return
        
        success = await self._process_event_skip(callback, message)
        if success:
            return

        await message.answer(ERROR_MESSAGE)

    async def dtls(self, callback: CallbackQuery) -> None:
        from_user = callback.from_user
        if from_user is None:
            self._logger.error("Callback user is missing")

            return

        message = self._require_message(callback)
        if message is None:
            return

        email_id = self._extract_email_id(callback.data or "")
        if email_id is None:
            await message.answer(ERROR_MESSAGE)
            self._logger.error("Failed to parse email id from callback data: %s", callback.data)

            return

        redis_key = f"{from_user.id}:{email_id}"
        details_str = self._redis_client.get(redis_key)
        if details_str is None:
            await message.answer(DETAILS_MISSING_MESSAGE)
            self._logger.error("details_str is None for email with id %s", email_id)

            return

        await callback.answer(DETAILS_PENDING_MESSAGE)

        self._refresh_details_cache(redis_key=redis_key, raw_details=details_str)

        details_text = self._prettify_details(details_str)
        for chunk in self._iter_chunks(details_text):
            await message.answer(chunk)

    async def _publish_application_update(self, callback: CallbackQuery, message: Message) -> bool:
        data = callback.data or ""

        email_id = self._extract_email_id(data)
        if email_id is None:
            await message.answer(ERROR_MESSAGE)
            self._logger.error("Failed to parse email id from callback data: %s", callback.data)

            return False

        prefix = self._extract_prefix(data)
        if prefix is None:
            await message.answer(ERROR_MESSAGE)
            self._logger.error("Failed to parse prefix from callback data: %s", callback.data)

            return False

        redis_key = self._build_redis_key(callback, email_id)
        if redis_key is None:
            return False

        details_str = self._redis_client.get(redis_key)
        if details_str is None:
            self._logger.error("details_str is None for email with id %s", email_id)

            return False

        await callback.answer("☑️ Confirming...")

        try:
            payload = json.loads(details_str)
            payload["action"] = self._confirmation_action()
        except Exception as e:
            await message.answer(ERROR_MESSAGE)
            self._logger.error("Failed to decode details_str JSON to an object: %s", e)

            return False

        topic = os.getenv(KAFKA_TOPIC_ENV, "")
        if not topic:
            await message.answer(ERROR_MESSAGE)
            self._logger.error("Environment variable %s is not set", KAFKA_TOPIC_ENV)

            return False

        try:
            self._kafka_client.publish(
                topic,
                self._confirmation_key(),
                payload,
                on_delivery=self._build_delivery_callback(),
            )
        except Exception as e:
            await message.answer(ERROR_MESSAGE)
            self._logger.error(
                "Failed to publish kafka event to topic [%s]: %s",
                topic,
                e,
            )

            return False

        await self._cleanup_buttons_on_skip(
            message=message,
            redis_key=redis_key,
        )

        return True

    async def _process_event_skip(self, callback: CallbackQuery, message: Message) -> bool:
        data = callback.data or ""

        skip_payload = self._parse_skip_callback(data)
        if skip_payload is None:
            self._logger.error("Failed to parse skip callback data: %s", data)

            return False

        prefix, email_id, reason = skip_payload
        if prefix is None:
            self._logger.error("Skip callback prefix is missing: %s", data)

            return False

        if email_id is None:
            self._logger.error("Skip callback email_id is missing: %s", data)

            return False

        if reason is None:
            keyboard = self._build_skip_reason_keyboard(prefix, email_id)

            await callback.answer()
            await message.answer(SKIP_PROMPT_MESSAGE, reply_markup=keyboard)

            return True

        await callback.answer()

        details = self._load_cached_details(callback=callback, email_id=email_id)
        if details is None:
            return False

        redis_key = self._build_redis_key(callback, email_id)
        if redis_key is None:
            return False

        user_email = self._extract_user_email(details)

        await self._handle_skip_reason(
            callback=callback,
            prefix=prefix,
            email_id=email_id,
            user_email=user_email,
            payload=details,
            reason=reason,
            message=message,
        )

        if reason == SKIP_REASON_BACK:
            target_message = self._resolve_root_message(message)
            if target_message is not message:
                await self._remove_skip_reason_prompt(message=message)

            return True

        await self._cleanup_buttons_on_skip(
            message=message,
            redis_key=redis_key,
        )

        target_message = self._resolve_root_message(message)
        if target_message is not message:
            await self._hide_skip_reason_keyboard(message=message)

        return True

    def _build_skip_reason_keyboard(self, prefix: str, email_id: str) -> InlineKeyboardMarkup:
        buttons = []
        for reason in SKIP_REASON_ORDER:
            text = SKIP_REASON_TEXTS[reason]
            callback_data = f"{prefix}:{SKIP_ACTION}:{email_id}:{reason}"
            button = InlineKeyboardButton(text=text, callback_data=callback_data)

            buttons.append(button)

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[button] for button in buttons],
        )

        return keyboard

    async def _handle_skip_reason(
        self,
        *,
        callback: CallbackQuery,
        prefix: str,
        email_id: str,
        user_email: str | None,
        payload: dict[str, Any],
        reason: str,
        message: Message,
    ) -> None:
        handlers = {
            SKIP_REASON_EMAIL_MISCLASSIFIED: self._handle_skip_email_misclassified,
            SKIP_REASON_APPLICATION_MISMATCH: self._handle_skip_application_mismatch,
            SKIP_REASON_OTHER: self._handle_skip_other_reason,
            SKIP_REASON_BACK: self._handle_skip_back,
        }

        handler = handlers.get(reason)
        if handler is None:
            self._logger.error(
                "Unsupported skip reason '%s' for prefix '%s'",
                reason,
                prefix,
            )

            return

        if reason != SKIP_REASON_BACK:
            await self._publish_feedback_event(
                callback=callback,
                email_id=email_id,
                reason=reason,
                user_email=user_email,
                payload=payload,
            )

        await handler(
            callback=callback,
            prefix=prefix,
            email_id=email_id,
            user_email=user_email,
            payload=payload,
            message=message,
        )

    def _parse_skip_callback(self, data: str) -> tuple[str | None, str | None, str | None] | None:
        parts = data.split(":")
        if len(parts) < 3:
            return None

        prefix, action, email_id, *rest = parts
        if action != SKIP_ACTION:
            return None

        reason = rest[0] if rest else None
        if reason == "":
            reason = None

        return prefix or None, email_id or None, reason

    def _build_redis_key(self, callback: CallbackQuery, email_id: str) -> str | None:
        from_user = callback.from_user
        if from_user is None:
            self._logger.error("Callback user is missing")

            return None

        redis_key = f"{from_user.id}:{email_id}"

        return redis_key

    def _load_cached_details(
        self,
        *,
        callback: CallbackQuery,
        email_id: str,
    ) -> dict[str, Any] | None:
        redis_key = self._build_redis_key(callback, email_id)
        if redis_key is None:
            return None

        details_str = self._redis_client.get(redis_key)
        if details_str is None:
            self._logger.error("details_str is None for email with id %s", email_id)

            return None

        try:
            payload = json.loads(details_str)

            return payload
        except Exception as e:
            self._logger.error("Failed to decode details_str JSON to an object: %s", e)

            return None

    def _extract_user_email(self, payload: dict[str, Any]) -> str | None:
        email = payload.get("email")
        if not isinstance(email, dict):
            return None

        recipient_email = email.get("recipient_email")
        if not isinstance(recipient_email, str) or not recipient_email:
            return None

        return recipient_email

    async def _publish_feedback_event(
        self,
        *,
        callback: CallbackQuery,
        email_id: str,
        reason: str,
        user_email: str | None,
        payload: dict[str, Any],
    ) -> None:
        topic = os.getenv(FEEDBACK_TOPIC_ENV, "")
        if not topic:
            self._logger.error(
                "Environment variable %s is not set",
                FEEDBACK_TOPIC_ENV,
            )

            return

        telegram_user = callback.from_user
        telegram_user_id = None
        telegram_username = None
        if telegram_user is not None:
            telegram_user_id = telegram_user.id
            telegram_username = telegram_user.username

        application_id = self._resolve_application_id(payload)

        try:
            email_id_int = int(email_id)
            feedback_key: int | str = email_id_int
            feedback_email_id: int | str = email_id_int
        except (TypeError, ValueError):
            feedback_key = email_id
            feedback_email_id = email_id

        timestamp = (
            datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

        message_payload = {
            "email_id": feedback_email_id,
            "application_id": application_id,
            "reason": reason,
            "user_email": user_email,
            "telegram_user": {
                "id": telegram_user_id,
                "username": telegram_username,
            },
            "payload": payload,
            "source": "telegram_bot",
            "created_at": timestamp,
        }

        try:
            self._kafka_client.publish(
                topic=topic,
                key=feedback_key,
                value=message_payload,
            )
            self._logger.info(
                "Published feedback event for email_id=%s reason=%s",
                email_id,
                reason,
            )
        except Exception as e:
            self._logger.error(
                "Failed to publish feedback event for email_id=%s: %s",
                email_id,
                e,
            )

    def _resolve_application_id(self, payload: dict[str, Any]) -> int | None:
        mapped_application = payload.get("mapped_application")
        if not isinstance(mapped_application, dict):
            return None

        raw_id = mapped_application.get("id")
        if raw_id is None:
            return None

        if not isinstance(raw_id, (str, int)):
            return None

        try:
            application_id = int(raw_id)
        except (TypeError, ValueError):
            return None

        return application_id

    async def _handle_skip_email_misclassified(
        self,
        *,
        callback: CallbackQuery,
        prefix: str,
        email_id: str,
        user_email: str | None,
        payload: dict[str, Any],
        message: Message,
    ) -> None:
        await self._send_skip_reason_ack(
            message=message,
            reason=SKIP_REASON_EMAIL_MISCLASSIFIED,
            user_email=user_email,
        )

        return None

    async def _handle_skip_application_mismatch(
        self,
        *,
        callback: CallbackQuery,
        prefix: str,
        email_id: str,
        user_email: str | None,
        payload: dict[str, Any],
        message: Message,
    ) -> None:
        await self._send_skip_reason_ack(
            message=message,
            reason=SKIP_REASON_APPLICATION_MISMATCH,
            user_email=user_email,
        )

        return None

    async def _handle_skip_other_reason(
        self,
        *,
        callback: CallbackQuery,
        prefix: str,
        email_id: str,
        user_email: str | None,
        payload: dict[str, Any],
        message: Message,
    ) -> None:
        await self._send_skip_reason_ack(
            message=message,
            reason=SKIP_REASON_OTHER,
            user_email=user_email,
        )

        return None

    async def _handle_skip_back(
        self,
        *,
        callback: CallbackQuery,
        prefix: str,
        email_id: str,
        user_email: str | None,
        payload: dict[str, Any],
        message: Message,
    ) -> None:
        return None

    async def _send_skip_reason_ack(
        self,
        *,
        message: Message,
        reason: str,
        user_email: str | None,
    ) -> None:
        text = SKIP_REASON_TEXTS.get(reason, "Unknown reason")
        email_value = user_email or "unknown email"
        response = f"{text}: {email_value}"

        await message.answer(response)

    async def _cleanup_buttons_on_skip(
        self,
        *,
        message: Message,
        redis_key: str | None,
    ) -> None:
        if redis_key is not None:
            self._redis_client.delete(redis_key)

        target_message = self._resolve_root_message(message)

        try:
            await target_message.edit_reply_markup(reply_markup=None)
        except Exception as e:
            self._logger.warning(
                "Failed to update inline keyboard for message %s: %s",
                target_message.message_id,
                e,
            )

    async def _hide_skip_reason_keyboard(
        self,
        *,
        message: Message,
    ) -> None:
        try:
            await message.edit_reply_markup(reply_markup=None)
        except Exception as e:
            self._logger.warning(
                "Failed to remove skip reason keyboard for message %s: %s",
                message.message_id,
                e,
            )

    async def _remove_skip_reason_prompt(
        self,
        *,
        message: Message,
    ) -> None:
        try:
            await message.delete()
        except Exception as e:
            self._logger.warning(
                "Failed to delete skip reason message %s: %s",
                message.message_id,
                e,
            )

    def _resolve_root_message(self, message: Message) -> Message:
        replied_message = message.reply_to_message
        if isinstance(replied_message, Message):
            return replied_message

        return message

    def _build_delivery_callback(self):
        def _callback(err: Exception | None, msg: Any) -> None:
            self._logger.info("kafka_publish_callback started")
            if err is not None:
                self._logger.error("Failed to publish message to Kafka: %s", err)

                return

            self._logger.info("Successfully published message to Kafka: %s", msg)

        return _callback

    def _require_message(self, callback: CallbackQuery) -> Message | None:
        message_like = callback.message
        if message_like is None:
            self._logger.error("Callback message is missing")

            return None

        if isinstance(message_like, Message):
            return message_like

        self._logger.error("Callback message is inaccessible")

        return None

    def _extract_prefix(self, data: str) -> str | None:
        parts = data.split(":")
        if not parts:
            return None

        prefix = parts[0]
        if not prefix:
            return None

        return prefix

    def _extract_email_id(self, data: str) -> str | None:
        parts = data.split(":")
        if not parts:
            return None

        email_id = parts[-1]
        if not email_id:
            return None

        return email_id

    def _prettify_details(self, details_str: str) -> str:
        try:
            obj = json.loads(details_str)
            pretty = json.dumps(obj, ensure_ascii=False, indent=2)

            return pretty
        except Exception as e:
            self._logger.warning("Failed to pretty-print JSON: %s", e)

            return details_str

    def _iter_chunks(self, text: str) -> Iterable[str]:
        if len(text) <= MAX_MESSAGE_CHARS:
            chunk = f"<pre>{text}</pre>"

            yield chunk

            return

        start = 0
        text_len = len(text)
        while start < text_len:
            end = min(start + MAX_MESSAGE_CHARS, text_len)
            if end < text_len:
                newline_index = text.rfind("\n", start, end)
                if newline_index != -1 and newline_index > start + (MAX_MESSAGE_CHARS // 2):
                    end = newline_index + 1

            chunk_body = text[start:end]
            chunk = f"<pre>{chunk_body}</pre>"

            yield chunk

            start = end

    def _refresh_details_cache(self, *, redis_key: str, raw_details: str) -> None:
        self._logger.debug("Skipping cache refresh for key %s", redis_key)

    def _confirmation_action(self) -> str:
        return "cnfm"

    def _confirmation_key(self) -> str:
        return "cnfm_button_pressed"


def build_button_handler_dependencies() -> tuple[KafkaClient, RedisClient]:
    kafka_client = get_kafka_client()
    redis_client = get_redis_client()

    return kafka_client, redis_client
