from dataclasses import dataclass


@dataclass
class SheetGetRequest:
    page: str
    id: str
    range: str
