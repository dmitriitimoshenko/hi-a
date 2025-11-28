import os

from dataclasses import dataclass


@dataclass(frozen=True)
class SheetsConfig:
    credentials_path: str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    timeout_secs: int = int(os.environ.get("GSHEETS_TIMEOUT_SECS", "30"))
    use_drive: bool = False


def get_sheets_config() -> SheetsConfig:
    return SheetsConfig()
