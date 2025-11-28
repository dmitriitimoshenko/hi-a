from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class KafkaConfig:
    bootstrap_servers: str
    client_id: str

    def producer_conf(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
        }


def get_kafka_config() -> KafkaConfig:
    bootstrap_servers = os.getenv("KAFKA_SERVER")
    client_id = os.getenv("KAFKA_CLIENT_ID")

    if not bootstrap_servers:
        message = "KAFKA_SERVER environment variable is not set"
        raise ValueError(message)

    if not client_id:
        message = "KAFKA_CLIENT_ID environment variable is not set"
        raise ValueError(message)

    config = KafkaConfig(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
    )

    return config
