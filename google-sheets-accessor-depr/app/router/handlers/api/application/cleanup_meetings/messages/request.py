from dataclasses import dataclass


@dataclass
class CleanupMeetingsRequest:
    id: str
    sheet_page: str = "applications_list"
