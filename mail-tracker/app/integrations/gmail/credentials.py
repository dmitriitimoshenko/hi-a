from dataclasses import dataclass


@dataclass(frozen=True)
class ImapCredentials:
    user: str
    app_password: str
    host: str = "imap.gmail.com"
    port: int = 993
    ssl: bool = True
