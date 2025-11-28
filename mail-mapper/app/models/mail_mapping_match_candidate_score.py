from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base

if TYPE_CHECKING:
    from app.models.mail_mapping_match_candidate import MailMappingMatchCandidate


class MailMappingMatchCandidateScore(Base):
    __tablename__ = "mail_mapping_match_candidate_scores"
    __table_args__ = (
        Index(
            "idx_mail_mapping_match_candidate_scores_candidate_id",
            "candidate_id",
        ),
        Index(
            "idx_mail_mapping_match_candidate_scores_candidate_type",
            "candidate_id",
            "score_type",
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
    candidate_id: Mapped[int] = mapped_column(
        ForeignKey(
            "mail_mapping_match_candidates.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    score_type: Mapped[str] = mapped_column(String(32), nullable=False)
    score: Mapped[float] = mapped_column(Float, nullable=False)
    weight: Mapped[float] = mapped_column(Float, nullable=False)

    candidate: Mapped["MailMappingMatchCandidate"] = relationship(
        back_populates="scores"
    )
