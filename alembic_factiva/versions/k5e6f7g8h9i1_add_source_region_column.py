"""add_source_region_column

Add source_region column to source_classification table.

Revision ID: k5e6f7g8h9i1
Revises: j4d5e6f7g8h0
Create Date: 2026-02-02 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "k5e6f7g8h9i1"
down_revision: Union[str, None] = "j4d5e6f7g8h0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add source_region column to source_classification table
    op.add_column(
        "source_classification",
        sa.Column("source_region", sa.String(), nullable=True),
    )


def downgrade() -> None:
    # Remove source_region column
    op.drop_column("source_classification", "source_region")
