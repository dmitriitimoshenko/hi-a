from dataclasses import dataclass


@dataclass
class ApplicationDiffRequestRange:
    sheet_page: str
    start_row: int
    end_row: int


@dataclass
class ApplicationDiffRequest:
    id: str
    sheet_range: ApplicationDiffRequestRange


__all__ = [
    "ApplicationDiffRequestRange",
    "ApplicationDiffRequest",
]
