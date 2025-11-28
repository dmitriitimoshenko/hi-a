"""add mail mapping feedback table

Revision ID: 20250105_010000
Revises: 20241028_120000
Create Date: 2025-01-05 01:00:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20250105_010000"
down_revision: Union[str, Sequence[str], None] = "20241028_120000"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mail_mapping_feedback",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "feedback_created_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("match_id", sa.BigInteger(), nullable=True),
        sa.Column(
            "matching_batch_id",
            postgresql.UUID(as_uuid=True),
            nullable=True,
        ),
        sa.Column("email_id", sa.BigInteger(), nullable=False),
        sa.Column("application_id", sa.BigInteger(), nullable=True),
        sa.Column("reason", sa.String(length=64), nullable=False),
        sa.Column("user_email", sa.String(length=255), nullable=True),
        sa.Column("telegram_user_id", sa.BigInteger(), nullable=True),
        sa.Column("telegram_username", sa.String(length=255), nullable=True),
        sa.Column(
            "source",
            sa.String(length=64),
            nullable=False,
            server_default="telegram_bot",
        ),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.ForeignKeyConstraint(
            ["match_id"],
            ["mail_mapping_matches.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_mail_mapping_feedback_email_id",
        "mail_mapping_feedback",
        ["email_id"],
        unique=False,
    )
    op.create_index(
        "idx_mail_mapping_feedback_application_id",
        "mail_mapping_feedback",
        ["application_id"],
        unique=False,
    )
    op.create_index(
        "idx_mail_mapping_feedback_match_id",
        "mail_mapping_feedback",
        ["match_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_mail_mapping_feedback_match_id",
        table_name="mail_mapping_feedback",
    )
    op.drop_index(
        "idx_mail_mapping_feedback_application_id",
        table_name="mail_mapping_feedback",
    )
    op.drop_index(
        "idx_mail_mapping_feedback_email_id",
        table_name="mail_mapping_feedback",
    )
    op.drop_table("mail_mapping_feedback")
