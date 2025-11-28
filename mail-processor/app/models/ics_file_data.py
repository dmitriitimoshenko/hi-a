from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class IcsFileData(Base):
    __tablename__ = "ics_file_data"

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

    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    content_type: Mapped[str] = mapped_column(String(127), nullable=False)
    disposition: Mapped[str | None] = mapped_column(String(127), nullable=True)
    method: Mapped[str | None] = mapped_column(String(127), nullable=True)
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)

    # Link to parent EmbdLrn. If parent is deleted, cascade delete ICS rows.
    embd_lrn_id: Mapped[int] = mapped_column(
        ForeignKey("embd_lrn.id", ondelete="CASCADE"), index=True, nullable=False
    )

    embd_lrn: Mapped["EmbdLrn"] = relationship(
        "EmbdLrn",
        foreign_keys=[embd_lrn_id],
        back_populates="ics_files",
        lazy="joined",
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "embd_lrn_id": self.embd_lrn_id,
            "filename": self.filename,
            "content_type": self.content_type,
            "disposition": self.disposition,
            "method": self.method,
            "size": self.size,
            "content": self.content,
        }
