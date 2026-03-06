import os

from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class KafkaConfig:
    bootstrap_servers: str
    client_id: str
    kafka_retries: int = 30

    def producer_conf(self) -> dict[str, Any]:
        conf = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "message.send.max.retries": self.kafka_retries,
        }

        return conf


def get_kafka_config() -> KafkaConfig:
    bootstrap_servers = os.getenv("KAFKA_SERVER", "")
    client_id = os.getenv("KAFKA_CLIENT_ID", "")

    if not bootstrap_servers:
        raise ValueError("Environment variable KAFKA_SERVER is not set")

    if not client_id:
        raise ValueError("Environment variable KAFKA_CLIENT_ID is not set")

    config = KafkaConfig(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
    )

    return config
