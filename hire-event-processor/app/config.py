import os


class Config:
    PORT = int(os.getenv("PORT", "8085"))

    KAFKA_SERVER = os.getenv("KAFKA_SERVER", "")
    KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "")
    KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "")
    KAFKA_TOPIC_INTERESTING_MAIL = os.getenv("KAFKA_TOPIC_INTERESTING_MAIL", "")
    KAFKA_TOPIC_HIRE_EVENT = os.getenv("KAFKA_TOPIC_HIRE_EVENT", "")
    KAFKA_TOPIC_APPLICATIONS_SYNC_UNPROCESSED = os.getenv(
        "KAFKA_TOPIC_APPLICATIONS_SYNC_UNPROCESSED", 
        "",
    )
    KAFKA_TOPIC_APPLICATIONS_SYNC_PROCESSED = os.getenv(
        "KAFKA_TOPIC_APPLICATIONS_SYNC_PROCESSED",
        "",
    )
    KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED = os.getenv(
        "KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED",
        "",
    )
    KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED = os.getenv(
        "KAFKA_TOPIC_APPLICATION_UPDATE_PROCESSED",
        "",
    )

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

    API_VERSION = os.getenv("API_VERSION", "1")

    GSA_BASE_URL = os.getenv("GSA_BASE_URL", "http://google-sheets-accessor:8083")
    SHEET_ID = os.getenv("SHEET_ID")
    GSA_REQUEST_TIMEOUT = float(os.getenv("GSA_REQUEST_TIMEOUT", "10.0"))

    SERVICE_NAME = "hep"
