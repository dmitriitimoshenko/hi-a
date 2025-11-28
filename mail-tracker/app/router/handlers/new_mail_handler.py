import logging
import re
from email.header import decode_header, make_header
from email.utils import getaddresses
from app.kafka_client import KafkaClient

logging.basicConfig(level=logging.INFO)


class NewMailHandler:
    def __init__(
        self,
        kafka_client: KafkaClient,
        topic: str,
        preview_chars: int = 5000,
    ) -> None:
        self._kafka_client = kafka_client
        self._topic = topic
        self._preview_chars = preview_chars
        self._logger = logging.getLogger(__name__)

    def _decode_header_value(self, raw) -> str:
        if raw is None:
            return ""
        return str(make_header(decode_header(raw)))

    def _extract_recipient(self, msg) -> tuple[str, str]:
        raw_to = msg.get("To", "")
        decoded_to = self._decode_header_value(raw_to)
        addrs = getaddresses([decoded_to])
        name, email_addr = ("", "")
        if addrs:
            name, email_addr = addrs[0]
        return name.strip(), email_addr.strip().lower()

    def _extract_sender(self, msg) -> tuple[str, str]:
        raw_from = msg.get("From", "")
        decoded_from = self._decode_header_value(raw_from)
        addrs = getaddresses([decoded_from])
        name, email_addr = ("", "")
        if addrs:
            name, email_addr = addrs[0]
        return name.strip(), email_addr.strip().lower()

    def _extract_subject(self, msg) -> str:
        return self._decode_header_value(msg.get("Subject", ""))

    def _payload_to_str(self, part) -> str | None:
        try:
            return part.get_content()
        except Exception:
            payload = part.get_payload(decode=True) or b""
            charset = (part.get_content_charset() or "utf-8").strip()
            return payload.decode(charset, errors="replace")

    def _extract_content(self, msg) -> tuple[str, str | None]:
        body_plain, body_html = None, None

        if hasattr(msg, "get_body"):
            preferred_plain = msg.get_body(preferencelist=("plain",))
            preferred_html = msg.get_body(preferencelist=("html",))
            if preferred_plain:
                body_plain = self._payload_to_str(preferred_plain)
            if preferred_html:
                body_html = self._payload_to_str(preferred_html)
        else:
            if msg.is_multipart():
                for part in msg.walk():
                    ctype = part.get_content_type()
                    disp = str(part.get("Content-Disposition", "")).lower()
                    if (
                        part.get_content_maintype() == "multipart"
                        or "attachment" in disp
                    ):
                        continue
                    if ctype == "text/plain" and body_plain is None:
                        body_plain = self._payload_to_str(part)
                    elif ctype == "text/html" and body_html is None:
                        body_html = self._payload_to_str(part)
            else:
                ctype = msg.get_content_type()
                if ctype == "text/plain":
                    body_plain = self._payload_to_str(msg)
                elif ctype == "text/html":
                    body_html = self._payload_to_str(msg)
                elif msg.get_content_maintype() == "text":
                    body_plain = self._payload_to_str(msg)

        if body_plain:
            return body_plain.strip(), "plain"
        elif body_html:
            text_from_html = re.sub(r"<\s*br\s*/?>", "\n", body_html, flags=re.I)
            text_from_html = re.sub(r"<[^>]+>", "", text_from_html)
            text_from_html = re.sub(r"&nbsp;", " ", text_from_html)
            return text_from_html.strip(), "html"

        return "", None

    def _extract_ics_files(self, msg) -> list[dict]:
        """Extracts ICS files (calendar invites) from the email message.

        Returns a list of dicts with:
        - filename: best-effort filename (or generated)
        - content_type: MIME type (e.g., text/calendar)
        - disposition: Content-Disposition if present
        - method: calendar method if provided as a parameter
        - size: size of the raw payload in bytes
        - content: decoded text content (utf-8 with replacement)
        """
        ics_list: list[dict] = []

        def is_ics_part(part) -> bool:
            if part.get_content_maintype() == "multipart":
                return False
            ctype = (part.get_content_type() or "").lower()
            filename = part.get_filename() or ""
            if ctype == "text/calendar":
                return True
            if ctype in {"application/ics", "application/x-ics", "text/x-vcalendar"}:
                return True
            if filename.lower().endswith(".ics"):
                return True
            return False

        parts = [msg]
        if hasattr(msg, "walk"):
            parts = list(msg.walk())

        idx = 0
        for part in parts:
            if not is_ics_part(part):
                continue

            # Fetch raw payload bytes
            payload_bytes = part.get_payload(decode=True)
            text_content = None
            if payload_bytes is None:
                # Fallback: try content manager for text
                try:
                    text_content = part.get_content()
                    payload_bytes = text_content.encode("utf-8", errors="replace")
                except Exception:
                    payload_bytes = b""

            # Decode text for JSON transport
            if text_content is None:
                charset = (part.get_content_charset() or "utf-8").strip()
                try:
                    text_content = (payload_bytes or b"").decode(
                        charset, errors="replace"
                    )
                except Exception:
                    text_content = (payload_bytes or b"").decode(
                        "utf-8", errors="replace"
                    )

            filename = part.get_filename()
            if not filename:
                idx += 1
                filename = f"calendar-{idx}.ics"

            ctype = (part.get_content_type() or "").lower()
            disp = str(part.get("Content-Disposition", "")) or None
            method = None
            try:
                method = part.get_param("method", header="content-type")
            except Exception:
                method = None

            ics_list.append(
                {
                    "filename": filename,
                    "content_type": ctype,
                    "disposition": disp,
                    "method": method,
                    "size": len(payload_bytes or b""),
                    "content": text_content,
                }
            )

        return ics_list

    def handle(self, msg, uid) -> None:
        recipient_name, recipient_email = self._extract_recipient(msg)
        sender_name, sender_email = self._extract_sender(msg)
        subject = self._extract_subject(msg)
        body_text, content_type = self._extract_content(msg)
        ics_files = self._extract_ics_files(msg)

        self._logger.info(
            "New email [%d] from: %s <%s> -> to: %s <%s> | subject: %s | type: %s",
            uid,
            sender_name,
            sender_email,
            recipient_name,
            recipient_email,
            subject,
            content_type or "unknown",
        )
        if ics_files:
            self._logger.info("Email [%d] contains %d ICS file(s)", uid, len(ics_files))
        self._logger.debug(
            "Body (first %d chars): %s",
            self._preview_chars,
            body_text[: self._preview_chars],
        )

        value = {
            "recipient_name": recipient_name,
            "recipient_email": recipient_email,
            "sender_name": sender_name,
            "sender_email": sender_email,
            "subject": subject,
            "body": body_text,
            "content_type": content_type,
            "ics_files": ics_files,
        }
        self._logger.info(f"Sending this to kafka {value}")

        self._kafka_client.publish(topic=self._topic, key=uid, value=value)

        self._logger.info(
            "Email [%d] processed and sent to Kafka topic '%s'", uid, self._topic
        )
