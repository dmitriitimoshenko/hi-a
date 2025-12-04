import os


class Config:
    KAFKA_SERVER = os.getenv("KAFKA_SERVER", "")
    KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "")
    KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "")
    KAFKA_TOPIC_NOTIFICATION = os.getenv("KAFKA_TOPIC_NOTIFICATION", "")
    KAFKA_TOPIC_NOTIFICATION_SYNC = os.getenv("KAFKA_TOPIC_NOTIFICATION_SYNC", "")
    KAFKA_TOPIC_FEEDBACK = os.getenv("KAFKA_TOPIC_FEEDBACK", "")

    BOT_TOKEN = os.getenv("BOT_TOKEN", "")

    GSA_BASE_URL = os.getenv(
        "GSA_BASE_URL",
        "http://google-sheets-accessor:8083",
    )
    API_VERSION = os.getenv("API_VERSION", "1")

    SERVICE_NAME = "tb"
