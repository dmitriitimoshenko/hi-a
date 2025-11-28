from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


@dataclass(slots=True)
class KafkaConfig:
    bootstrap_servers: str
    client_id: str
    group_id: str
    kafka_debug: bool = False
    kafka_reset_from_beginning: bool = False

    def producer_conf(self) -> dict[str, Any]:
        config = {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
        }

        return config


def get_kafka_config() -> KafkaConfig:
    config = KafkaConfig(
        bootstrap_servers=os.getenv("KAFKA_SERVER", ""),
        client_id=os.getenv("KAFKA_CLIENT_ID", ""),
        group_id=os.getenv("KAFKA_CONSUMER_GROUP", ""),
    )

    return config
