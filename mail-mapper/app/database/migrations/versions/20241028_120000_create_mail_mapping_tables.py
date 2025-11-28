"""create mail mapping tables

Revision ID: 20241028_120000
Revises:
Create Date: 2025-10-26 21:30:00.000000
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision: str = "20241028_120000"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "mail_mapping_matches",
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
            "matching_batch_id",
            postgresql.UUID(as_uuid=True),
            nullable=False,
        ),
        sa.Column("email_id", sa.BigInteger(), nullable=False),
        sa.Column("application_id", sa.BigInteger(), nullable=True),
        sa.Column("email_label", sa.String(length=64), nullable=False),
        sa.Column("min_match_score", sa.Float(), nullable=False),
        sa.Column("overall_score", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_mail_mapping_matches_application_id",
        "mail_mapping_matches",
        ["application_id"],
        unique=False,
    )
    op.create_index(
        "idx_mail_mapping_matches_email_application",
        "mail_mapping_matches",
        ["email_id", "application_id"],
        unique=False,
    )
    op.create_index(
        "idx_mail_mapping_matches_email_id",
        "mail_mapping_matches",
        ["email_id"],
        unique=False,
    )

    op.create_table(
        "mail_mapping_match_candidates",
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
        sa.Column("match_id", sa.BigInteger(), nullable=False),
        sa.Column("application_id", sa.BigInteger(), nullable=False),
        sa.Column("aggregated_score", sa.Float(), nullable=False),
        sa.Column("rank", sa.Integer(), nullable=False),
        sa.Column(
            "is_selected",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["match_id"],
            ["mail_mapping_matches.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_mail_mapping_match_candidates_match_app",
        "mail_mapping_match_candidates",
        ["match_id", "application_id"],
        unique=False,
    )
    op.create_index(
        "idx_mail_mapping_match_candidates_match_id",
        "mail_mapping_match_candidates",
        ["match_id"],
        unique=False,
    )

    op.create_table(
        "mail_mapping_match_candidate_scores",
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
        sa.Column("candidate_id", sa.BigInteger(), nullable=False),
        sa.Column("score_type", sa.String(length=32), nullable=False),
        sa.Column("score", sa.Float(), nullable=False),
        sa.Column("weight", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(
            ["candidate_id"],
            ["mail_mapping_match_candidates.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "idx_mail_mapping_match_candidate_scores_candidate_id",
        "mail_mapping_match_candidate_scores",
        ["candidate_id"],
        unique=False,
    )
    op.create_index(
        "idx_mail_mapping_match_candidate_scores_candidate_type",
        "mail_mapping_match_candidate_scores",
        ["candidate_id", "score_type"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "idx_mail_mapping_match_candidate_scores_candidate_type",
        table_name="mail_mapping_match_candidate_scores",
    )
    op.drop_index(
        "idx_mail_mapping_match_candidate_scores_candidate_id",
        table_name="mail_mapping_match_candidate_scores",
    )
    op.drop_table("mail_mapping_match_candidate_scores")

    op.drop_index(
        "idx_mail_mapping_match_candidates_match_id",
        table_name="mail_mapping_match_candidates",
    )
    op.drop_index(
        "idx_mail_mapping_match_candidates_match_app",
        table_name="mail_mapping_match_candidates",
    )
    op.drop_table("mail_mapping_match_candidates")

    op.drop_index(
        "idx_mail_mapping_matches_email_id",
        table_name="mail_mapping_matches",
    )
    op.drop_index(
        "idx_mail_mapping_matches_email_application",
        table_name="mail_mapping_matches",
    )
    op.drop_index(
        "idx_mail_mapping_matches_application_id",
        table_name="mail_mapping_matches",
    )
    op.drop_table("mail_mapping_matches")
