"""add_source_owner_column

Add source_owner column to source_classification table to track media ownership.

Revision ID: h2b3c4d5e6f8
Revises: g1a2b3c4d5e7
Create Date: 2026-01-15 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "h2b3c4d5e6f8"
down_revision: Union[str, None] = "g1a2b3c4d5e7"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add source_owner column to source_classification table
    op.add_column(
        "source_classification",
        sa.Column("source_owner", sa.String(), nullable=True),
    )


def downgrade() -> None:
    # Remove source_owner column
    op.drop_column("source_classification", "source_owner")
