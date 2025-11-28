from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class KafkaConfig:
    bootstrap_servers: str
    client_id: str

    def producer_conf_as_dict(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
        }
