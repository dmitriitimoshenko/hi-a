import os


class Config:
    PORT = int(os.getenv("PORT", "8086"))

    KAFKA_SERVER = os.getenv("KAFKA_SERVER", "")
    KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "")
    KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "")
    KAFKA_TOPIC_HIRE_EVENT = os.getenv("KAFKA_TOPIC_HIRE_EVENT", "")
    KAFKA_TOPIC_NOTIFICATION = os.getenv("KAFKA_TOPIC_NOTIFICATION", "")
    KAFKA_TOPIC_APPLICATIONS_SYNC_PROCESSED = os.getenv(
        "KAFKA_TOPIC_APPLICATIONS_SYNC_PROCESSED",
        "",
    )
    KAFKA_TOPIC_NOTIFICATION_SYNC = os.getenv("KAFKA_TOPIC_NOTIFICATION_SYNC", "")

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

    API_VERSION = os.getenv("API_VERSION", "1")

    SERVICE_NAME = "hen"
