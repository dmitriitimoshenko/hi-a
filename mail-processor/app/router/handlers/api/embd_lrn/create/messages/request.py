from typing import Any
from dataclasses import dataclass


@dataclass
class EmbdLrnCreateRequest:
    label: str
    items: list[Any]
