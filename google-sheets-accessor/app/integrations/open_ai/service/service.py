import logging

from app.integrations.open_ai.client.client import OpenAIClient, get_openai_client

logging.basicConfig(level=logging.INFO)


class OpenAIService:
    def __init__(self, client: OpenAIClient):
        self._client = client
        self._logger = logging.getLogger(__name__)

    def get_embedding(
        self, text: str, model: str = "text-embedding-3-small"
    ) -> list[float]:
        response = self._client.get_embedding(text, model)

        return response.data[0].embedding


def get_openai_service(client: OpenAIClient | None = None) -> OpenAIService:
    resolved_client = client or get_openai_client()

    return OpenAIService(resolved_client)
