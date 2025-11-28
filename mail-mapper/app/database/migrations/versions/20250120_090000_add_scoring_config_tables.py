"""add scoring config tables and metadata columns

Revision ID: 20250120_090000
Revises: 20250105_010000
Create Date: 2025-01-20 09:00:00.000000
"""

from datetime import datetime
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "20250120_090000"
down_revision: Union[str, Sequence[str], None] = "20250105_010000"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "scoring_weights",
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
        sa.Column("version", sa.String(length=128), nullable=False),
        sa.Column("description", sa.String(length=512), nullable=True),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("version", name="uq_scoring_weights_version"),
    )
    op.create_index(
        "idx_scoring_weights_active",
        "scoring_weights",
        ["is_active"],
        unique=False,
    )

    op.create_table(
        "scoring_weight_entries",
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
        sa.Column("weight_id", sa.BigInteger(), nullable=False),
        sa.Column("component", sa.String(length=64), nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.ForeignKeyConstraint(
            ["weight_id"],
            ["scoring_weights.id"],
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "weight_id",
            "component",
            name="uq_scoring_weight_entries_component",
        ),
    )
    op.create_index(
        "idx_scoring_weight_entries_weight",
        "scoring_weight_entries",
        ["weight_id"],
        unique=False,
    )

    op.create_table(
        "scoring_calibrations",
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
        sa.Column("version", sa.String(length=128), nullable=False),
        sa.Column("description", sa.String(length=512), nullable=True),
        sa.Column("bias", sa.Float(), nullable=False),
        sa.Column("scale", sa.Float(), nullable=False),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("version", name="uq_scoring_calibrations_version"),
    )
    op.create_index(
        "idx_scoring_calibrations_active",
        "scoring_calibrations",
        ["is_active"],
        unique=False,
    )

    op.add_column(
        "mail_mapping_matches",
        sa.Column("raw_overall_score", sa.Float(), nullable=True),
    )
    op.add_column(
        "mail_mapping_matches",
        sa.Column("scoring_config_version", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "mail_mapping_matches",
        sa.Column("calibration_version", sa.String(length=128), nullable=True),
    )

    op.add_column(
        "mail_mapping_match_candidates",
        sa.Column("raw_aggregated_score", sa.Float(), nullable=True),
    )
    op.add_column(
        "mail_mapping_match_candidates",
        sa.Column("scoring_config_version", sa.String(length=128), nullable=True),
    )
    op.add_column(
        "mail_mapping_match_candidates",
        sa.Column("calibration_version", sa.String(length=128), nullable=True),
    )

    bind = op.get_bind()
    timestamp = datetime.utcnow()

    bind.execute(
        sa.text(
            """
            UPDATE mail_mapping_matches
            SET raw_overall_score = overall_score,
                scoring_config_version = :version
            """
        ),
        {"version": "static-default"},
    )

    bind.execute(
        sa.text(
            """
            UPDATE mail_mapping_match_candidates
            SET raw_aggregated_score = aggregated_score,
                scoring_config_version = :version
            """
        ),
        {"version": "static-default"},
    )

    weight_id = bind.execute(
        sa.text(
            """
            INSERT INTO scoring_weights (created_at, updated_at, version, description, is_active)
            VALUES (:created_at, :updated_at, :version, :description, true)
            RETURNING id
            """
        ),
        {
            "created_at": timestamp,
            "updated_at": timestamp,
            "version": "static-default",
            "description": "Initial static weights migrated from constants",
        },
    ).scalar_one()

    default_weights = {
        "similarity": 0.44,
        "time": 0.20,
        "sender": 0.36,
    }

    for component, value in default_weights.items():
        bind.execute(
            sa.text(
                """
                INSERT INTO scoring_weight_entries (
                    created_at,
                    updated_at,
                    weight_id,
                    component,
                    value
                )
                VALUES (:created_at, :updated_at, :weight_id, :component, :value)
                """
            ),
            {
                "created_at": timestamp,
                "updated_at": timestamp,
                "weight_id": weight_id,
                "component": component,
                "value": value,
            },
        )


def downgrade() -> None:
    op.drop_column("mail_mapping_match_candidates", "calibration_version")
    op.drop_column("mail_mapping_match_candidates", "scoring_config_version")
    op.drop_column("mail_mapping_match_candidates", "raw_aggregated_score")

    op.drop_column("mail_mapping_matches", "calibration_version")
    op.drop_column("mail_mapping_matches", "scoring_config_version")
    op.drop_column("mail_mapping_matches", "raw_overall_score")

    op.drop_index("idx_scoring_calibrations_active", table_name="scoring_calibrations")
    op.drop_table("scoring_calibrations")

    op.drop_index("idx_scoring_weight_entries_weight", table_name="scoring_weight_entries")
    op.drop_table("scoring_weight_entries")

    op.drop_index("idx_scoring_weights_active", table_name="scoring_weights")
    op.drop_table("scoring_weights")
