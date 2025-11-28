from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    Float,
    ForeignKey,
    Index,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class ScoringWeightEntry(Base):
    __tablename__ = "scoring_weight_entries"
    __table_args__ = (
        Index("idx_scoring_weight_entries_weight", "weight_id"),
        UniqueConstraint(
            "weight_id",
            "component",
            name="uq_scoring_weight_entries_component",
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
    weight_id: Mapped[int] = mapped_column(
        ForeignKey(
            "scoring_weights.id",
            ondelete="CASCADE",
        ),
        nullable=False,
    )
    component: Mapped[str] = mapped_column(String(64), nullable=False)
    value: Mapped[float] = mapped_column(Float, nullable=False)

    weight = relationship("ScoringWeight", back_populates="entries")

