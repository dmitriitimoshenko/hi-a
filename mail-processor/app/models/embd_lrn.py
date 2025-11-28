from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Integer, String, Text, JSON, DateTime, func
from pgvector.sqlalchemy import Vector
from datetime import datetime

from app.database import Base


class EmbdLrn(Base):
    __tablename__ = "embd_lrn"

    id: Mapped[int] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    inbox_email_id: Mapped[int | None] = mapped_column(
        Integer, unique=True, nullable=True
    )
    label: Mapped[str] = mapped_column(String(255))

    subject: Mapped[str] = mapped_column(Text)
    body: Mapped[str] = mapped_column(Text)
    content: Mapped[str] = mapped_column(Text)

    sender_email: Mapped[str] = mapped_column(String(255))
    sender_name: Mapped[str] = mapped_column(String(255))
    recipient_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    recipient_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    content_type: Mapped[str | None] = mapped_column(String(64))
    meta: Mapped[dict | None] = mapped_column(JSON)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(dim=1536))
    used_for_learning: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    should_be_sent_to_heh: Mapped[bool] = mapped_column(nullable=False, default=True)

    # Related ICS files extracted from the email
    ics_files: Mapped[list["IcsFileData"]] = relationship(
        "IcsFileData",
        back_populates="embd_lrn",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "inbox_email_id": self.inbox_email_id,
            "label": self.label,
            "subject": self.subject,
            "body": self.body,
            "content": self.content,
            "sender_email": self.sender_email,
            "sender_name": self.sender_name,
            "recipient_name": self.recipient_name,
            "recipient_email": self.recipient_email,
            "content_type": self.content_type,
            "meta": self.meta,
            "used_for_learning": self.used_for_learning.isoformat()
            if self.used_for_learning
            else None,
            "should_be_sent_to_heh": self.should_be_sent_to_heh,
        }
