from dataclasses import dataclass


@dataclass
class ApplicationFetchRequestRange:
    sheet_page: str
    start_ceil: str | None = None
    end_ceil: str | None = None


@dataclass
class ApplicationFetchRequest:
    id: str
    sheet_range: ApplicationFetchRequestRange
