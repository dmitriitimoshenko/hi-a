"""add cluster support to embd_cntr

Revision ID: 4ae8f3f2db52
Revises: 422e7a47abd3
Create Date: 2025-09-12 10:15:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "4ae8f3f2db52"
down_revision: Union[str, Sequence[str], None] = "422e7a47abd3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        "embd_cntr",
        sa.Column(
            "cluster_id",
            sa.Integer(),
            nullable=True,
        ),
    )
    op.add_column(
        "embd_cntr",
        sa.Column(
            "threshold",
            sa.Float(),
            nullable=True,
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("embd_cntr", "threshold")
    op.drop_column("embd_cntr", "cluster_id")
