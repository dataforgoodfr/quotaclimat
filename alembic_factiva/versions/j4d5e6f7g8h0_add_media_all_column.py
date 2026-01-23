"""add_media_all_column

Add media_all column to source_classification table for media grouping.

Revision ID: j4d5e6f7g8h0
Revises: i3c4d5e6f7g9
Create Date: 2026-01-23 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "j4d5e6f7g8h0"
down_revision: Union[str, None] = "i3c4d5e6f7g9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add media_all column to source_classification table
    op.add_column(
        "source_classification",
        sa.Column("media_all", sa.String(), nullable=True),
    )


def downgrade() -> None:
    # Remove media_all column
    op.drop_column("source_classification", "media_all")
