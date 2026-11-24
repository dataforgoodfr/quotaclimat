"""Add media_export column to ad

Revision ID: b8d2f4a6c1e3
Revises: c5e7a9b1d3f2
Create Date: 2026-11-24

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "b8d2f4a6c1e3"
down_revision: Union[str, None] = "c5e7a9b1d3f2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "ad",
        sa.Column("media_export", postgresql.JSONB(), nullable=True),
        schema="advertising",
    )


def downgrade() -> None:
    op.drop_column("ad", "media_export", schema="advertising")
