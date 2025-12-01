from dataclasses import dataclass
from typing import Any


@dataclass
class ApplicationUpdateExternalRequest:
    sheet_page: str
    db_snapshot: dict[str, Any]


__all__ = [
    "ApplicationUpdateExternalRequest",
]
