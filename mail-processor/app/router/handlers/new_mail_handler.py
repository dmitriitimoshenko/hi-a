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
    ):
        self._kafka_client = kafka_client
        self._topic = topic
        self._preview_chars = preview_chars
        self._logger = logging.getLogger(__name__)

    def _decode_header_value(self, raw) -> str:
        if raw is None:
            return ""
        return str(make_header(decode_header(raw)))

    def _extract_sender(self, msg):
        raw_from = msg.get("From", "")
        decoded_from = self._decode_header_value(raw_from)
        addrs = getaddresses([decoded_from])
        name, email_addr = ("", "")
        if addrs:
            name, email_addr = addrs[0]
        return name.strip(), email_addr.strip().lower()

    def _extract_subject(self, msg):
        return self._decode_header_value(msg.get("Subject", ""))

    def _payload_to_str(self, part):
        try:
            return part.get_content()
        except Exception:
            payload = part.get_payload(decode=True) or b""
            charset = (part.get_content_charset() or "utf-8").strip()
            return payload.decode(charset, errors="replace")

    def _extract_content(self, msg):
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

    def handle(self, msg, uid):
        sender_name, sender_email = self._extract_sender(msg)
        subject = self._extract_subject(msg)
        body_text, content_type = self._extract_content(msg)

        self._logger.info(
            "New email [%d] from: %s <%s> | subject: %s | type: %s",
            uid,
            sender_name,
            sender_email,
            subject,
            content_type or "unknown",
        )
        self._logger.debug(
            "Body (first %d chars): %s",
            self._preview_chars,
            body_text[: self._preview_chars],
        )

        self._kafka_client.publish(
            topic=self._topic,
            key=uid,
            value={
                "sender_name": sender_name,
                "sender_email": sender_email,
                "subject": subject,
                "body": body_text,
                "content_type": content_type,
            },
        )

        self._logger.info(
            "Email [%d] processed and sent to Kafka topic '%s'", uid, self._topic
        )
