from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    JSON,
    String,
    func,
)
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class MailMappingFeedback(Base):
    __tablename__ = "mail_mapping_feedback"

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
    feedback_created_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    match_id: Mapped[int | None] = mapped_column(
        ForeignKey(
            "mail_mapping_matches.id",
            ondelete="SET NULL",
        ),
        nullable=True,
    )
    matching_batch_id: Mapped[uuid.UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=True,
    )
    email_id: Mapped[int] = mapped_column(BigInteger, nullable=False, index=True)
    application_id: Mapped[int | None] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )
    reason: Mapped[str] = mapped_column(String(64), nullable=False)
    user_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    telegram_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    telegram_username: Mapped[str | None] = mapped_column(String(255), nullable=True)
    source: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        default="telegram_bot",
    )
    payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    match: Mapped["MailMappingMatch"] = relationship(
        "MailMappingMatch",
        back_populates="feedback_entries",
    )
