import os

from dataclasses import dataclass


@dataclass(slots=True)
class OpenAIConfig:
    api_key: str


def get_openai_cfg() -> OpenAIConfig:
    return OpenAIConfig(os.getenv("OPENAI_API_KEY"))
