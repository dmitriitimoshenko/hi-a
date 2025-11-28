import os


class Config:
    GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
    GMAIL_USER = os.getenv("GMAIL_USER")

    PORT = int(os.getenv("PORT", 8082))

    KAFKA_TOPIC_NEW_MAIL = os.getenv("KAFKA_TOPIC_NEW_MAIL", "new-mail")
    KAFKA_SERVER = os.getenv("KAFKA_SERVER")
    KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID")

    SERVICE_NAME = "mt"

    API_VERSION = os.getenv("API_VERSION", "1")
