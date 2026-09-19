import os


class Config:
    GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD")
    GMAIL_USER = os.getenv("GMAIL_USER")

    PORT = int(os.getenv("PORT", 8082))

    STREAM_NEW_MAIL = os.getenv("STREAM_NEW_MAIL", "new-mail")
    REDIS_URL = os.getenv("REDIS_URL")
    STREAM_CONSUMER_ID = os.getenv("STREAM_CONSUMER_ID")

    SERVICE_NAME = "mt"

    API_VERSION = os.getenv("API_VERSION", "1")
