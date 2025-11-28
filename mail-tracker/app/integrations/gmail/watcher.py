from __future__ import annotations
import time
import email
import logging
import threading
from collections import OrderedDict
from typing import Callable, Iterable, cast
from email import policy
from email.message import Message
from imapclient import IMAPClient
from app.redis_client import RedisClient
from .credentials import ImapCredentials
from .config import ImapWatchConfig

logging.basicConfig(level=logging.INFO)


class GmailImapIdleWatcher:
    def __init__(
        self,
        redis_client: RedisClient,
        creds: ImapCredentials,
        config: ImapWatchConfig,
        on_message: Callable[[Message, int], None],
    ) -> None:
        self._redis_client = redis_client
        self._creds = creds
        self._cfg = config
        self._on_message = on_message
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._uidvalidity: int = 0
        self._idling: bool = False
        self._seen_uids: OrderedDict[int, None] = OrderedDict()
        self._seen_cap: int = 1024
        self._logger = logging.getLogger(__name__)

    def start(self, daemon: bool = True) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="GmailImapIdleWatcher", daemon=daemon
        )
        self._thread.start()

    def stop(self, join: bool = True, timeout: float | None = None) -> None:
        self._stop.set()
        if self._thread and join:
            self._thread.join(timeout=timeout)

    def run_blocking(self) -> None:
        self._run_loop()

    def _connect_and_select(self) -> IMAPClient:
        server = IMAPClient(
            self._creds.host, ssl=self._creds.ssl, port=self._creds.port
        )
        server.login(self._creds.user, self._creds.app_password)
        server.select_folder(self._cfg.folder, readonly=self._cfg.readonly)
        return server

    def _init_last_uid(self, server: IMAPClient) -> None:
        last_seen_uid_str = self._redis_client.get("email:last_uid") or "0"
        if int(last_seen_uid_str) == 0:
            status = cast(
                dict[bytes | str, int],
                server.folder_status(self._cfg.folder, [b"UIDNEXT", b"UIDVALIDITY"]),
            )
            uidnext = int(status.get(b"UIDNEXT", status.get("UIDNEXT", 0)))
            self._uidvalidity = int(
                status.get(b"UIDVALIDITY", status.get("UIDVALIDITY", 0))
            )
            uidlast = max(uidnext - 1, 0)
            self._redis_client.set("email:last_uid", str(uidlast))
            self._logger.debug(
                "Initialized from STATUS: uidvalidity=%s last_seen_uid=%s (uidnext=%s)",
                self._uidvalidity,
                uidlast,
                uidnext,
            )

    def _handle_new_uids(self, server: IMAPClient, uids: Iterable[int]) -> None:
        if not uids:
            return
        unique_sorted: list[int] = []
        for uid in sorted(set(uids)):
            if uid in self._seen_uids:
                self._logger.debug("Skip duplicate uid=%s (already seen)", uid)
                continue
            unique_sorted.append(uid)

        if not unique_sorted:
            return

        fetch = cast(
            dict[int, dict[bytes | str, bytes]],
            server.fetch(unique_sorted, [b"RFC822"]),
        )

        processed_uids: list[int] = []
        for uid in unique_sorted:
            data = fetch.get(uid)
            if not data:
                self._logger.warning("No IMAP data for uid=%s", uid)
                continue

            raw_rfc822 = cast(bytes | None, data.get(b"RFC822"))
            if raw_rfc822 is None:
                raw_rfc822 = cast(bytes | None, data.get("RFC822"))

            if raw_rfc822 is None:
                self._logger.warning("Missing RFC822 payload for uid=%s", uid)
                continue

            msg = email.message_from_bytes(raw_rfc822, policy=policy.default)
            last_seen_uid_str = self._redis_client.get("email:last_uid") or "0"
            if uid <= int(last_seen_uid_str):
                self._logger.debug(
                    "Skip old uid %d when last seen uid is %s", uid, last_seen_uid_str
                )
                processed_uids.append(uid)
                continue

            try:
                self._on_message(msg, uid)
            except Exception:
                self._logger.exception("on_message failed for uid=%s", uid)
                continue

            processed_uids.append(uid)

        if not processed_uids:
            return

        for uid in processed_uids:
            self._seen_uids[uid] = None
            if len(self._seen_uids) > self._seen_cap:
                self._seen_uids.popitem(last=False)

        uidlast = max(processed_uids)
        self._redis_client.set("email:last_uid", str(uidlast))

    def _enter_idle(self, server: IMAPClient) -> float:
        if not self._idling:
            server.idle()
            self._idling = True
        return time.time()

    def _exit_idle(self, server: IMAPClient) -> None:
        if self._idling:
            try:
                server.idle_done()
            except Exception:
                pass
            finally:
                self._idling = False

    def _run_idle_cycle(self, server: IMAPClient) -> None:
        idle_started = self._enter_idle(server)
        try:
            while not self._stop.is_set():
                responses = server.idle_check(timeout=self._cfg.idle_check_timeout_sec)

                if responses:
                    self._logger.debug("IDLE responses: %s", responses)

                    last_seen_uid = int(self._redis_client.get("email:last_uid") or "0")

                    self._exit_idle(server)

                    new_uids = cast(
                        list[int], server.search(f"UID {last_seen_uid + 1}:*")
                    )
                    if new_uids:
                        self._handle_new_uids(server, new_uids)

                    idle_started = self._enter_idle(server)

                if time.time() - idle_started > self._cfg.idle_reissue_sec:
                    self._exit_idle(server)
                    try:
                        server.noop()
                    except Exception:
                        pass
                    idle_started = self._enter_idle(server)
        finally:
            self._exit_idle(server)

    def _run_loop(self) -> None:
        backoff = self._cfg.reconnect_backoff_start_sec
        while not self._stop.is_set():
            try:
                with self._connect_and_select() as server:
                    self._logger.info(
                        "IMAP connected to %s/%s", self._creds.host, self._cfg.folder
                    )
                    self._init_last_uid(server)
                    backoff = self._cfg.reconnect_backoff_start_sec
                    self._run_idle_cycle(server)
            except Exception as e:
                if self._stop.is_set():
                    break
                self._logger.warning("IMAP error: %s. Reconnecting in %ss…", e, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, self._cfg.reconnect_backoff_cap_sec)
        self._logger.info("GmailImapIdleWatcher stopped.")
