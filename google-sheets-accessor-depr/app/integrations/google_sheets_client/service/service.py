import logging
from typing import Any, Sequence

from app.integrations.google_sheets_client.client.client import (
    SheetsClient,
    get_sheets_client,
)

logging.basicConfig(level=logging.INFO)


class SheetsService:
    def __init__(self, sheets_client: SheetsClient) -> None:
        self._logging = logging.getLogger(__name__)
        self._sheets_client = sheets_client

    def get_values(self, spreadsheet_id: str, a1_range: str) -> list[list[str]]:
        vals = self._sheets_client.get_values(spreadsheet_id, a1_range)
        return vals

    def update_values(
        self, spreadsheet_id: str, sheet_page: str, ceil: str, val: str
    ) -> None:
        self._sheets_client.update_values(
            spreadsheet_id, f"{sheet_page}!{ceil}", [[val]], True
        )

    def batch_update_values(
        self,
        spreadsheet_id: str,
        sheet_page: str,
        updates: Sequence[tuple[str, Any]],
    ) -> None:
        if not updates:
            return

        data = [
            {
                "range": f"{sheet_page}!{cell}",
                "values": [[value]],
            }
            for cell, value in updates
        ]

        self._sheets_client.batch_update_values(
            spreadsheet_id,
            data,
            True,
        )


def get_sheets_service() -> SheetsService:
    client = get_sheets_client()
    return SheetsService(client)
