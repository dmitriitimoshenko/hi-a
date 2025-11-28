from dataclasses import dataclass


@dataclass(frozen=True)
class ImapWatchConfig:
    folder: str = "INBOX"
    readonly: bool = True
    idle_reissue_sec: int = 5 * 60
    idle_check_timeout_sec: int = 5
    reconnect_backoff_start_sec: int = 3
    reconnect_backoff_cap_sec: int = 60
