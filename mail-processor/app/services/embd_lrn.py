import logging
from datetime import datetime
from typing import Iterable
from zoneinfo import ZoneInfo
from fastapi import Depends
from sqlalchemy.orm import Session

from app.config import Config
from app.database import get_db
from app.enums import EmailLabel
from app.integrations.open_ai.service.service import (
    OpenAIService,
    provide_openai_service,
)
from app.models import EmbdCntr, EmbdLrn
from app.tools.normalization import normalize_content
from app.thresholds import resolve_threshold

logging.basicConfig(level=logging.INFO)
CET_ZONE = ZoneInfo("CET")


class EmbdLrnService:
    def __init__(
        self,
        db: Session,
        openai_service: OpenAIService,
        config: Config | None = None,
    ) -> None:
        self._db = db
        self._openai_service = openai_service
        self._logger = logging.getLogger(__name__)
        self._config = config or Config()

    def create_with_label(self, items: list[dict], label: EmailLabel) -> None:
        items_to_save: list[EmbdLrn] = []

        for item in items:
            if not isinstance(item, list) or len(item) != 4:
                continue

            sender_email = item[0]
            sender_name = item[1]
            subject = item[2]
            body = item[3]

            content = f"SUBJECT {subject} BODY {body}"

            items_to_save.append(
                EmbdLrn(
                    label=label,
                    subject=subject,
                    body=body,
                    content=content,
                    sender_email=sender_email,
                    sender_name=sender_name,
                    should_be_sent_to_heh=False,
                )
            )

        try:
            self._db.add_all(items_to_save)
            self._db.commit()
            self._logger.debug(
                f"Saved items on create_with_label: {len(items_to_save)}"
            )
        except Exception as e:
            e_msg = f"Failed to create_with_label: {e}"
            self._logger.debug(e_msg)
            raise ValueError(e_msg)

    def learn(self) -> None:
        try:
            items = (
                self._db.query(EmbdLrn).where(EmbdLrn.used_for_learning == None).all()
            )
            if not items:
                msg = "No EmbdLrn not used_for_learning found"
                logging.warning(msg)
                return
        except Exception as e:
            msg = "Failed to get EmbdLrn not used_for_learning"
            self._logger.error(msg)
            raise ValueError(msg)

        try:
            contents = [normalize_content(item.content) for item in items]

            self._logger.info(f"contents after normalization: {contents}")

            embeddings = self._openai_service.get_embeddings(contents)
        except Exception as e:
            msg = f"Error during learning: {e}"
            self._logger.error(msg)
            raise ValueError(msg)

        try:
            for item, embedding in zip(items, embeddings):
                item.embedding = embedding
                self._db.add(item)

            self._db.commit()

            self._logger.debug(
                "Received embeddings for %d items of %d tokens",
                len(embeddings),
                len(embeddings[0]) if embeddings else 0,
            )
        except Exception as e:
            msg = "Failed to save a list of EmbdLrn with update embeddings"
            self._logger.error(msg)
            raise ValueError(msg)

    def commit(self) -> None:
        try:
            items = (
                self._db.query(EmbdLrn).order_by(EmbdLrn.id.desc()).limit(1000).all()
            )
            if not items:
                msg = "No EmbdLrn found"
                logging.warning(msg)
                return
        except Exception as e:
            msg = "Failed to get any EmbdLrn"
            self._logger.error(msg)
            raise ValueError(msg)

        try:
            filtered_items: dict[str, list[list[float]]] = {}

            now = datetime.now(tz=CET_ZONE)
            for item in items:
                item.used_for_learning = now
                self._db.add(item)

                if item.label not in filtered_items:
                    filtered_items[item.label] = []
                if item.embedding is None:
                    msg = f"Embedding is not set on email with id {item.id}"
                    self._logger.error(msg)
                    raise ValueError(msg)

                filtered_items[item.label].append(item.embedding)

            batch_size = max(
                3, self._config.CLASSIFY_BY_CENTROID_COMMIT_BATCH_SIZE
            )

            available_labels = EmailLabel.get_all_end_labels()
            for label in available_labels:
                embeddings = filtered_items.get(label, [])
                if not embeddings:
                    continue

                clusters = list(self._split_embeddings(embeddings, batch_size))

                (
                    self._db.query(EmbdCntr)
                    .where(EmbdCntr.label == label)
                    .delete(synchronize_session=False)
                )

                for cluster_index, cluster_embeddings in enumerate(
                    clusters,
                    start=1,
                ):
                    centroid_vector = self._openai_service.get_embedding_centroid(
                        cluster_embeddings
                    )
                    threshold_value = self._calculate_cluster_threshold(
                        label,
                        centroid_vector,
                        cluster_embeddings,
                    )

                    embd_cntr = EmbdCntr(
                        label=label,
                        cluster_id=cluster_index,
                        embedding=centroid_vector,
                        threshold=threshold_value,
                    )
                    self._db.add(embd_cntr)

                    self._logger.debug(
                        "Prepared centroid for %s cluster %d with %d samples (threshold=%.4f)",
                        label,
                        cluster_index,
                        len(cluster_embeddings),
                        threshold_value,
                    )

            self._db.commit()

            self._logger.debug("Committed learning for %d items", len(items))
        except Exception as e:
            msg = f"Error while committing EmbdLrn: {e}"
            self._logger.error(msg)
            raise ValueError(msg)

    def _split_embeddings(
        self,
        embeddings: list[list[float]],
        cluster_size: int,
    ) -> Iterable[list[list[float]]]:
        for start in range(0, len(embeddings), cluster_size):
            yield embeddings[start : start + cluster_size]

    def _calculate_cluster_threshold(
        self,
        label: EmailLabel | str,
        centroid: list[float],
        embeddings: list[list[float]],
    ) -> float:
        cluster_size = len(embeddings)
        if cluster_size < 2:
            label_key = label.value if isinstance(label, EmailLabel) else str(label)
            threshold = resolve_threshold(
                label_key,
                self._config.CLASSIFY_BY_CENTROID_THRESHOLDS,
                self._config.CLASSIFY_BY_CENTROID_THRESHOLD,
            )

            return float(threshold)

        similarities = [
            self._openai_service.compare_vectors(centroid, embedding, normalize=True)
            for embedding in embeddings
        ]

        min_similarity = min(similarities)
        threshold_multiplier = (
            self._config.CLASSIFY_BY_CENTROID_THRESHOLD_MULTIPLIER
        )
        threshold = min_similarity * threshold_multiplier

        return float(threshold)


def get_embd_service(
    db: Session = Depends(get_db),
    openai_service: OpenAIService = Depends(provide_openai_service),
) -> EmbdLrnService:
    config = Config()

    return EmbdLrnService(db, openai_service, config)
