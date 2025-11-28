from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    func,
    String,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.mail_mapping_match import MailMappingMatch
    from app.models.mail_mapping_match_candidate_score import (
        MailMappingMatchCandidateScore,
    )


class MailMappingMatchCandidate(Base):
    __tablename__ = "mail_mapping_match_candidates"
    __table_args__ = (
        Index("idx_mail_mapping_match_candidates_match_id", "match_id"),
        Index(
            "idx_mail_mapping_match_candidates_match_app",
            "match_id",
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
    match_id: Mapped[int] = mapped_column(
        ForeignKey(
            "mail_mapping_matches.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    application_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    aggregated_score: Mapped[float] = mapped_column(Float, nullable=False)
    raw_aggregated_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    scoring_config_version: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )
    calibration_version: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )
    rank: Mapped[int] = mapped_column(Integer, nullable=False)
    is_selected: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    match: Mapped["MailMappingMatch"] = relationship(back_populates="candidates")
    scores: Mapped[list["MailMappingMatchCandidateScore"]] = relationship(
        back_populates="candidate",
        cascade="all, delete-orphan",
    )
