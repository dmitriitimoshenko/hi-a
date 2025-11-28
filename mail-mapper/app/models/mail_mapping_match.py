from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import BigInteger, DateTime, Float, Index, String, func
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.mail_mapping_match_candidate import MailMappingMatchCandidate
    from app.models.mail_mapping_feedback import MailMappingFeedback


class MailMappingMatch(Base):
    __tablename__ = "mail_mapping_matches"
    __table_args__ = (
        Index("idx_mail_mapping_matches_email_id", "email_id"),
        Index("idx_mail_mapping_matches_application_id", "application_id"),
        Index(
            "idx_mail_mapping_matches_email_application",
            "email_id",
            "application_id",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    matching_batch_id: Mapped[uuid.UUID] = mapped_column(
        PGUUID(as_uuid=True),
        default=uuid.uuid4,
        nullable=False,
    )
    email_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    application_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    email_label: Mapped[str] = mapped_column(String(64), nullable=False)
    min_match_score: Mapped[float] = mapped_column(Float, nullable=False)
    overall_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    raw_overall_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    scoring_config_version: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )
    calibration_version: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )

    candidates: Mapped[list["MailMappingMatchCandidate"]] = relationship(
        back_populates="match",
        cascade="all, delete-orphan",
    )
    feedback_entries: Mapped[list["MailMappingFeedback"]] = relationship(
        back_populates="match",
        cascade="all, delete-orphan",
    )
