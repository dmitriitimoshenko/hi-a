from __future__ import annotations
from datetime import datetime
from typing import Optional
from pgvector.sqlalchemy import Vector
from sqlalchemy import JSON, String, DateTime, func, ForeignKey, Integer
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Application(Base):
    __tablename__ = "application"

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

    company: Mapped[str] = mapped_column(String(255), nullable=False)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    employment_type: Mapped[str] = mapped_column(String(64), nullable=False)
    work_mode: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(64), nullable=False)
    applied_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    responded_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    next_follow_up_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    stage: Mapped[str | None] = mapped_column(String(64), nullable=True)
    meta: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(dim=1536))

    # not listed in sheet
    row_id: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)

    applied_email_received: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    applied_email_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    denied_email_received: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    denied_email_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    meeting_inv_email_received: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    meeting_inv_email_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    meeting_crt_email_received: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    meeting_crt_email_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    meeting_upd_email_received: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    meeting_upd_email_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    meeting_cncl_email_received: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    meeting_cncl_email_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    salary_applied_id: Mapped[int | None] = mapped_column(
        ForeignKey("salary.id", ondelete="SET NULL"), index=True
    )
    salary_proposed_id: Mapped[int | None] = mapped_column(
        ForeignKey("salary.id", ondelete="SET NULL"), index=True
    )

    salary_applied: Mapped[Optional["Salary"]] = relationship(
        "Salary",
        foreign_keys=[salary_applied_id],
        back_populates="applications_as_applied",
        lazy="joined",
    )
    salary_proposed: Mapped[Optional["Salary"] | None] = relationship(
        "Salary",
        foreign_keys=[salary_proposed_id],
        back_populates="applications_as_proposed",
        lazy="joined",
    )
