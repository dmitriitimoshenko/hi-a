from __future__ import annotations
from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, DateTime, func, Numeric, CheckConstraint
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import Base


class Salary(Base):
    __tablename__ = "salary"

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

    amount_from: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=True)
    amount_to: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=True)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    period: Mapped[str] = mapped_column(String(32), nullable=False)

    applications_as_applied: Mapped[list["Application"]] = relationship(
        "Application",
        back_populates="salary_applied",
        foreign_keys="Application.salary_applied_id",
        cascade="save-update",
    )
    applications_as_proposed: Mapped[list["Application"]] = relationship(
        "Application",
        back_populates="salary_proposed",
        foreign_keys="Application.salary_proposed_id",
        cascade="save-update",
    )

    __table_args__ = (
        CheckConstraint(
            "(amount_from IS NULL) OR (amount_to IS NULL) OR (amount_to >= amount_from)",
            name="ck_salary_amount_from_non_bigger_than_amount_to",
        ),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "amount_from": float(self.amount_from)
            if self.amount_from is not None
            else None,
            "amount_to": float(self.amount_to) if self.amount_to is not None else None,
            "currency": self.currency,
            "period": self.period,
        }

    def __repr__(self) -> str:
        return f"<Salary id={self.id} {self.amount} {self.currency} ({self.period})>"

    def is_cut_range(self) -> bool:
        return (
            self.amount_from is not None
            and self.amount_to is not None
            and self.amount_from < self.amount_to
        )

    def is_single(self) -> bool:
        return (
            self.amount_from is not None
            and self.amount_to is not None
            and self.amount_from == self.amount_to
        )
