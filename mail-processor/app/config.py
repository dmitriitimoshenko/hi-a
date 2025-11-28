import os

from app.thresholds import load_thresholds


class Config:
    PORT = int(os.getenv("PORT", 8081))

    KAFKA_SERVER = os.getenv("KAFKA_SERVER")
    KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID")
    KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP")
    KAFKA_TOPIC_NEW_MAIL = os.getenv("KAFKA_TOPIC_NEW_MAIL", "")
    KAFKA_TOPIC_INTERESTING_MAIL = os.getenv("KAFKA_TOPIC_INTERESTING_MAIL", "")

    OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

    API_VERSION = os.getenv("API_VERSION", "1")

    SERVICE_NAME = "mp"

    CLASSIFY_BY_CENTROID_THRESHOLD = float(
        os.getenv("CLASSIFY_BY_CENTROID_THRESHOLD", "0.55")
    )
    CLASSIFY_BY_CENTROID_THRESHOLDS = load_thresholds(
        CLASSIFY_BY_CENTROID_THRESHOLD
    )
    CLASSIFY_BY_CENTROID_MARGIN = float(
        os.getenv("CLASSIFY_BY_CENTROID_MARGIN", "0.035")
    )
    CLASSIFY_BY_CENTROID_COMMIT_BATCH_SIZE = int(
        os.getenv("CLASSIFY_BY_CENTROID_COMMIT_BATCH_SIZE", "10")
    )
    CLASSIFY_BY_CENTROID_THRESHOLD_MULTIPLIER = float(
        os.getenv("CLASSIFY_BY_CENTROID_THRESHOLD_MULTIPLIER", "0.8")
    )

    HUNGARIAN_MIN_MATCH_SCORE = float(
        os.getenv("HUNGARIAN_MIN_MATCH_SCORE", "0.5")
    )

    MAIL_MAPPER_URL = os.getenv("MAIL_MAPPER_URL", "http://mail-mapper:8088")
    MAIL_MAPPER_TIMEOUT = float(os.getenv("MAIL_MAPPER_TIMEOUT", "30.0"))
