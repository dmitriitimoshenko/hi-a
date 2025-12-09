import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from html import escape
from typing import Any

from aiogram import Bot
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from icalendar import Calendar

from app.enums import EmailLabel
from app.redis_client import RedisClient

logging.basicConfig(level=logging.INFO)


APPLICATION_DIFF_CALLBACK_PREFIX = "application_diff"
APPLICATION_DIFF_REDIS_PREFIX = "application_diff"
APPLICATION_DIFF_CACHE_TTL_SECONDS = 7 * 24 * 60 * 60
MAX_DIFF_VALUE_LENGTH = 400
TRUNCATION_SUFFIX = "… (truncated)"


@dataclass
class MessageContext:
    text: str
    keyboard: InlineKeyboardMarkup | None
    ttl_seconds: int | None
    should_cache: bool


class KafkaNotificationHandler:
    def __init__(
        self,
        redis_client: RedisClient,
    ) -> None:
        self._redis_client = redis_client
        self._logger = logging.getLogger(__name__)

    async def handle_notification(
        self,
        bot: Bot,
        topic: str,
        key: int,
        value: dict[str, Any],
    ) -> None:
        if not isinstance(value, dict):
            self._logger.warning(
                "Received non-dict payload from topic %s: %s",
                topic,
                value,
            )

            return

        message_type = value.get("type")

        if message_type == "application_diff":
            await self._handle_application_diff(bot, value)

            return

        email = value.get("email") or {}
        mapped_application = value.get("mapped_application") or {}

        tg_id = os.getenv("TG_ID")
        if tg_id is not None:
            tg_id = int(tg_id)

        payload = json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode(
            "utf-8"
        )

        label_value = email.get("label")
        try:
            label = EmailLabel(label_value)
        except Exception:
            self._logger.warning("Invalid Email Label: %s", label_value)
            label = None

        context = self._build_message_context(
            label,
            email,
            mapped_application,
            key,
        )

        if tg_id is not None and context.should_cache:
            cache_key = f"{tg_id}:{key}"
            self._store_payload(cache_key, payload, context.ttl_seconds)

        if tg_id is not None:
            await bot.send_message(
                chat_id=tg_id,
                text=context.text,
                reply_markup=context.keyboard,
            )
            return

    async def _handle_application_diff(
        self,
        bot: Bot,
        payload: dict[str, Any],
    ) -> None:
        tg_id_env = os.getenv("TG_ID")
        if tg_id_env is None:
            self._logger.warning(
                "Cannot deliver application diff notification: TG_ID env is not set",
            )

            return

        try:
            tg_id = int(tg_id_env)
        except Exception:
            self._logger.error("TG_ID is not a valid integer: %s", tg_id_env)

            return

        event_id = payload.get("event_id")
        if not isinstance(event_id, str):
            self._logger.error("Application diff payload has invalid event_id: %s", payload)

            return

        message = self._build_application_diff_message(payload)
        keyboard = self._build_application_diff_keyboard(event_id)

        cache_key = f"{APPLICATION_DIFF_REDIS_PREFIX}:{event_id}"
        serialized_payload = json.dumps(payload, ensure_ascii=False)

        try:
            self._redis_client.set(
                cache_key,
                serialized_payload,
                ex=APPLICATION_DIFF_CACHE_TTL_SECONDS,
            )
        except Exception as e:
            self._logger.error(
                "Failed to cache application diff payload event_id=%s: %s",
                event_id,
                e,
            )

            return

        await bot.send_message(
            chat_id=tg_id,
            text=message,
            reply_markup=keyboard,
        )

        self._logger.info(
            "Delivered application diff notification event_id=%s to tg_id=%s",
            event_id,
            tg_id,
        )

    def _build_application_diff_message(self, payload: dict[str, Any]) -> str:
        company = escape(payload.get("company") or "Unknown company")
        role = escape(payload.get("role_title") or "Unknown role")
        differences = payload.get("differences") or []
        errors = payload.get("errors") or []
        detected_at = payload.get("detected_at")
        row_id = payload.get("row_id")

        diff_lines = self._format_diff_lines(differences)

        text_parts = [
            (
                f"In application (row {row_id}) for role <b>{role}</b> in company <b>{company}</b> "
                "we noticed the following changes:"
            ),
            "\n".join(diff_lines),
        ]

        if errors:
            error_lines = "\n".join(
                f"• {escape(str(error))}"
                for error in errors
            )
            text_parts.append("\n<b>Additional notes</b>:")
            text_parts.append(error_lines)

        if detected_at:
            text_parts.append(
                f"\nDetected at: <code>{escape(str(detected_at))}</code>"
            )

        text_parts.append(
            "\nApply updates or skip."
        )

        message = "\n".join(part for part in text_parts if part)

        return message

    def _format_diff_lines(
        self,
        differences: list[dict[str, Any]],
    ) -> list[str]:
        if not differences:
            return ["• No detailed field differences provided"]

        lines: list[str] = []

        for diff in differences:
            if not isinstance(diff, dict):
                continue

            field = escape(str(diff.get("field") or "unknown_field"))
            db_value = self._stringify_diff_value(diff.get("db_value"))
            sheet_value = self._stringify_diff_value(diff.get("sheet_value"))

            line = f"• <code>{field}</code>: {db_value} → {sheet_value}"
            lines.append(line)

        if not lines:
            lines.append("• No detailed field differences provided")

        return lines

    def _stringify_diff_value(self, value: Any) -> str:
        if value is None:
            return "—"

        if isinstance(value, (dict, list)):
            try:
                serialized = json.dumps(value, ensure_ascii=False)
            except Exception:
                serialized = str(value)
        else:
            serialized = str(value)
        normalized = serialized.strip()
        max_length = MAX_DIFF_VALUE_LENGTH
        suffix = TRUNCATION_SUFFIX

        if len(normalized) > max_length:
            cutoff = max_length - len(suffix)
            cutoff = cutoff if cutoff > 0 else max_length
            normalized = f"{normalized[:cutoff].rstrip()}{suffix}"

        result = escape(normalized)

        return result

    def _build_application_diff_keyboard(self, event_id: str) -> InlineKeyboardMarkup:
        apply_sheet_button = InlineKeyboardButton(
            text="Apply Google Sheet",
            callback_data=f"{APPLICATION_DIFF_CALLBACK_PREFIX}:aplsh:{event_id}",
        )
        apply_internal_button = InlineKeyboardButton(
            text="Apply internal",
            callback_data=f"{APPLICATION_DIFF_CALLBACK_PREFIX}:aplin:{event_id}",
        )
        skip_button = InlineKeyboardButton(
            text="Skip",
            callback_data=f"{APPLICATION_DIFF_CALLBACK_PREFIX}:skp:{event_id}",
        )

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[apply_sheet_button, apply_internal_button, skip_button]],
        )

        return keyboard

    def _build_message_context(
        self,
        label: EmailLabel | None,
        email: dict[str, Any],
        mapped_application: dict[str, Any],
        email_id: int,
    ) -> MessageContext:
        message = "..."
        keyboard = None
        ttl_seconds: int | None = None
        should_cache = False

        month_ttl = self._month_in_seconds()

        if label is None:
            context = MessageContext(message, keyboard, ttl_seconds, should_cache)

            return context

        sender_name = escape(email.get("sender_name") or "")
        sender_email = escape(email.get("sender_email") or "")
        title = escape(mapped_application.get("title") or "")
        company = escape(mapped_application.get("company") or "")

        match label:
            case EmailLabel.APPLIED:
                should_cache = True
                ttl_seconds = month_ttl
                message = (
                    f"You received an email from {sender_name} ({sender_email}), "
                    f"that tells you have applied on position <b>{title}</b> at "
                    f"<b>{company}</b>\n\n"
                    "Please confirm if I understood everything correctly or let me skip this case"
                )

                keyboard = self._build_keyboard("applied", email_id)

            case EmailLabel.DENIED:
                should_cache = True
                ttl_seconds = month_ttl

                message = (
                    f"You received an email from {sender_name} ({sender_email}), "
                    f"that tells that you application on role <b>{title}</b> at "
                    f"<b>{company}</b> was denied\n\n"
                    "Please confirm if I understood everything correctly or let me skip this case"
                )

                keyboard = self._build_keyboard("denied", email_id)

            case EmailLabel.MEETING_INV:
                should_cache = True
                ttl_seconds = month_ttl

                message = (
                    f"You received an email from {sender_name} ({sender_email}), "
                    f"that tells that you were invited to a meeting for role <b>{title}</b> at "
                    f"<b>{company}</b>\n\n"
                    "Please confirm if I understood everything correctly or let me skip this case"
                )

                keyboard = self._build_keyboard("meeting_inv", email_id)

            case EmailLabel.MEETING_CRT:
                should_cache = True
                ttl_seconds = month_ttl

                meeting_dt = self._extract_meeting_creation_time(email)

                message = (
                    f"You received an email from {sender_name} ({sender_email}), "
                    f"that tells that a meeting was scheduled ({meeting_dt}) to talk with you about role <b>{title}</b> at "
                    f"<b>{company}</b>\n\n"
                    "Please confirm if I understood everything correctly or let me skip this case"
                )

                keyboard = self._build_keyboard("meeting_crt", email_id)

            case EmailLabel.MEETING_UPD:
                should_cache = True
                ttl_seconds = month_ttl

                meeting_dt = self._extract_meeting_update_time(email)

                message = (
                    f"You received an email from {sender_name} ({sender_email}), "
                    f"that tells that a meeting was <b>RE</b>-scheduled ({meeting_dt}) to talk with you about role <b>{title}</b> at "
                    f"<b>{company}</b>\n\n"
                    "Please confirm if I understood everything correctly or let me skip this case"
                )

                keyboard = self._build_keyboard("meeting_upd", email_id)

            case EmailLabel.OFFER:
                should_cache = True
                ttl_seconds = month_ttl

                message = (
                    f"Waaait... Is it true??"
                    f"It seems you received an email from {sender_name} ({sender_email}), "
                    f"that tells you got an OFFER 🎉🎉🎉 for role <b>{title}</b> at "
                    f"<b>{company}</b>!\n\n"
                    "Please confirm if I understood everything correctly or let me skip this case"
                )

                keyboard = self._build_keyboard("meeting_cncl", email_id)

            case _:
                self._logger.warning("Invalid Email Label: %s", label)

        context = MessageContext(message, keyboard, ttl_seconds, should_cache)

        return context

    def _build_keyboard(
        self, 
        prefix: str, 
        email_id: int,
        should_hide_cnfm: bool = False,
        should_hide_skp: bool = False,
    ) -> InlineKeyboardMarkup:
        inline_keyboard_content = []

        if not should_hide_cnfm:
            inline_keyboard_content.append(
                InlineKeyboardButton(
                    text="Confirm",
                    callback_data=f"{prefix}:cnfm:{email_id}",
                )
            )

        inline_keyboard_content.append(
            InlineKeyboardButton(
                text="Details",
                callback_data=f"{prefix}:dtls:{email_id}",
            )
        )

        if not should_hide_skp:
            inline_keyboard_content.append(
                InlineKeyboardButton(
                    text="Skip",
                    callback_data=f"{prefix}:skp:{email_id}",
                )
            )

        inline_keyboard = [inline_keyboard_content] 

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=inline_keyboard,
        )

        return keyboard

    def _store_payload(self, key: str, payload: bytes, ttl_seconds: int | None) -> None:
        self._redis_client.set(key, payload, ttl_seconds)

    def _month_in_seconds(self) -> int:
        seconds = int(timedelta(days=30).total_seconds())

        return seconds

    def _extract_meeting_creation_time(self, email: dict[str, Any]) -> str:
        ics_list = []
        try:
            ics_list = email.get("ics_file_data_list") or []
        except Exception:
            ics_list = []

        meeting_dt_str = "UNKNOWN"

        request_ics = next(
            (item for item in ics_list if (item or {}).get("method") == "REQUEST"), None
        )
        if request_ics and isinstance(request_ics, dict):
            content = request_ics.get("content") or ""

            self._logger.info("Found content for meeting creation: %s", content)

            try:
                calendar = Calendar.from_ical(content)
                event = next((component for component in calendar.walk("vevent")), None)
                if event is not None:
                    dtstart = event.get("dtstart")
                    if dtstart is not None:
                        meeting_dt_str = self._format_calendar_dt(dtstart)
            except Exception:
                self._logger.warning("Failed to parse ICS content for meeting creation")

        return meeting_dt_str

    def _extract_meeting_update_time(self, email: dict[str, Any]) -> str:
        ics_list = []
        try:
            ics_list = (email or {}).get("ics_files") or []
        except Exception:
            ics_list = []

        meeting_dt_str = "UNKNOWN"

        try:
            request_ics = next(
                (item for item in ics_list if (item or {}).get("method") == "REQUEST"),
                None,
            )
            if request_ics and isinstance(request_ics, dict):
                content = request_ics.get("content") or ""
                match = re.search(
                    r"^DTSTART(?:;TZID=[^:]+)?:([0-9]{8}T[0-9]{6})(Z)?$",
                    content,
                    re.MULTILINE,
                )
                if match:
                    timestamp = match.group(1)
                    tz_suffix = match.group(2) or ""
                    meeting_dt_str = self._parse_ics_timestamp(timestamp, tz_suffix)
        except Exception:
            meeting_dt_str = "UNKNOWN"

        return meeting_dt_str

    def _parse_ics_timestamp(self, timestamp: str, tz_suffix: str) -> str:
        try:
            dt = datetime.strptime(timestamp, "%Y%m%dT%H%M%S")
            if tz_suffix == "Z":
                dt = dt.replace(tzinfo=timezone.utc)

            formatted = dt.strftime("%Y-%m-%d %H:%M:%S")

            return formatted
        except Exception:
            return timestamp

    def _format_calendar_dt(self, dtstart: Any) -> str:
        dt_value = getattr(dtstart, "dt", dtstart)

        if isinstance(dt_value, datetime):
            formatted = dt_value.strftime("%Y-%m-%d %H:%M:%S")

            return formatted

        combined = datetime.combine(dt_value, datetime.min.time())
        formatted = combined.strftime("%Y-%m-%d %H:%M:%S")

        return formatted


def get_kafka_notification_handler(
    redis_client: RedisClient,
) -> KafkaNotificationHandler:
    handler = KafkaNotificationHandler(
        redis_client=redis_client,
    )

    return handler
