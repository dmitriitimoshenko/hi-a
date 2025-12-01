from dataclasses import dataclass


@dataclass
class SheetGetRequest:
    id: str
    range: str
