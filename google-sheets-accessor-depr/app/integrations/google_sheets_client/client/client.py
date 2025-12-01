from __future__ import annotations

import time
import logging

from typing import Sequence, Any
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from .config import SheetsConfig, get_sheets_config

SCOPES_SHEETS_RW = ["https://www.googleapis.com/auth/spreadsheets"]
SCOPES_DRIVE_RO = ["https://www.googleapis.com/auth/drive.readonly"]


class SheetsClient:
    def __init__(self, config: SheetsConfig = SheetsConfig()):
        if not config.credentials_path:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS is not specified")

        scopes: list[str] = SCOPES_SHEETS_RW.copy()
        if config.use_drive:
            scopes += SCOPES_DRIVE_RO

        creds = Credentials.from_service_account_file(
            config.credentials_path, scopes=scopes
        )
        self._sheets = build(
            "sheets", "v4", credentials=creds, cache_discovery=False
        ).spreadsheets()
        self._drive = None
        if config.use_drive:
            self._drive = build("drive", "v3", credentials=creds, cache_discovery=False)

        self._timeout_secs = config.timeout_secs

        self._logger = logging.getLogger(__name__)

    # ------------------------- INTERNAL: retries -------------------------

    def _with_retry(self, func, *args, **kwargs):
        deadline = time.time() + self._timeout_secs
        delay = 0.5
        attempt = 0
        while True:
            try:
                return func(*args, **kwargs)
            except HttpError as e:
                status = (
                    getattr(e, "status_code", None)
                    or getattr(e, "resp", {}).get("status")
                    or None
                )
                status = int(status) if status is not None else None
                retriable = status in (429, 500, 502, 503, 504)
                if not retriable:
                    raise ValueError("Desided to do no retries")
                attempt += 1
                now = time.time()
                if now + delay > deadline:
                    msg = (
                        "Sheets retry deadline exceeded after %d attempts; re-raising.",
                        attempt,
                    )
                    self._logger.warning(msg)
                    raise ValueError(msg)
                self._logger.warning(
                    "Sheets transient error %s, retry in %.1fs (attempt %d)...",
                    status,
                    delay,
                    attempt,
                )
                time.sleep(delay)
                delay = min(delay * 2, 5.0)

    # ---------------------------- MAIN PART ------------------------------

    def get_values(self, spreadsheet_id: str, a1_range: str) -> list[list[str]]:
        req = self._sheets.values().get(spreadsheetId=spreadsheet_id, range=a1_range)
        resp = self._with_retry(req.execute)
        return resp.get("values", [])

    def batch_get_values(
        self, spreadsheet_id: str, ranges: Sequence[str]
    ) -> dict[str, list[list[str]]]:
        req = self._sheets.values().batchGet(
            spreadsheetId=spreadsheet_id, ranges=list(ranges)
        )
        resp = self._with_retry(req.execute)
        result: dict[str, list[list[str]]] = {}
        for value_range in resp.get("valueRanges", []):
            result[value_range["range"]] = value_range.get("values", [])
        return result

    def update_values(
        self,
        spreadsheet_id: str,
        a1_range: str,
        values_2d: Sequence[Sequence[Any]],
        user_entered: bool = True,
    ) -> None:
        value_input_option = "USER_ENTERED" if user_entered else "RAW"
        body = {"values": [list(row) for row in values_2d]}
        req = self._sheets.values().update(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            valueInputOption=value_input_option,
            body=body,
        )
        self._with_retry(req.execute)

    def append_rows(
        self,
        spreadsheet_id: str,
        a1_range: str,
        rows: Sequence[Sequence[Any]],
        user_entered: bool = True,
        insert_rows: bool = True,
    ) -> None:
        value_input_option = "USER_ENTERED" if user_entered else "RAW"
        insert_option = "INSERT_ROWS" if insert_rows else "OVERWRITE"
        body = {"values": [list(row) for row in rows]}
        req = self._sheets.values().append(
            spreadsheetId=spreadsheet_id,
            range=a1_range,
            valueInputOption=value_input_option,
            insertDataOption=insert_option,
            body=body,
        )
        self._with_retry(req.execute)

    def clear_range(self, spreadsheet_id: str, a1_range: str) -> None:
        req = self._sheets.values().clear(
            spreadsheetId=spreadsheet_id, range=a1_range, body={}
        )
        self._with_retry(req.execute)

    def batch_update_values(
        self,
        spreadsheet_id: str,
        data: Sequence[dict[str, Any]],
        user_entered: bool = True,
    ) -> None:
        value_input_option = "USER_ENTERED" if user_entered else "RAW"
        body = {"valueInputOption": value_input_option, "data": data}
        req = self._sheets.values().batchUpdate(spreadsheetId=spreadsheet_id, body=body)
        self._with_retry(req.execute)


def get_sheets_client() -> SheetsClient:
    cfg = get_sheets_config()
    return SheetsClient(cfg)
