from typing import Optional

from openai import OpenAI

from .config import OpenAIConfig, get_openai_cfg


class OpenAIClient:
    def __init__(self, config: OpenAIConfig):
        self.config = config
        self._client = OpenAI(api_key=self.config.api_key)

    def get_embedding(self, text: list[str] | str, model="text-embedding-3-small"):
        if isinstance(text, str):
            text = text.replace("\n", " ")
        else:
            for i in range(len(text)):
                text[i] = text[i].replace("\n", " ")

        response = self._client.embeddings.create(
            model=model,
            input=text,
        )
        return response


def get_openai_client(config: Optional[OpenAIConfig] = None) -> OpenAIClient:
    cfg = config or get_openai_cfg()

    return OpenAIClient(cfg)
