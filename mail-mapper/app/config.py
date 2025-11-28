import os


class Config:
    PORT = int(os.getenv("PORT", "8088"))

    API_VERSION = os.getenv("API_VERSION", "1")
    SERVICE_NAME = "mail-mapper"

    GSA_BASE_URL = os.getenv(
        "GSA_BASE_URL",
        "http://google-sheets-accessor:8083",
    )
    GSA_TIMEOUT = float(os.getenv("GSA_TIMEOUT", "30.0"))

    KAFKA_SERVER = os.getenv("KAFKA_SERVER", "")
    KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "")
    KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "")
    KAFKA_TOPIC_FEEDBACK = os.getenv("KAFKA_TOPIC_FEEDBACK", "")

    HUNGARIAN_MIN_MATCH_SCORE = float(
        os.getenv("HUNGARIAN_MIN_MATCH_SCORE", "0.5")
    )

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
