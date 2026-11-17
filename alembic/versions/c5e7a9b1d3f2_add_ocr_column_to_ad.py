"""Add ocr_transcript column to ad

Revision ID: c5e7a9b1d3f2
Revises: f4a8c2e6d1b3
Create Date: 2026-10-27

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "c5e7a9b1d3f2"
down_revision: Union[str, None] = "f4a8c2e6d1b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "ad",
        sa.Column("ocr_transcript", sa.Text(), nullable=True),
        schema="advertising",
    )


def downgrade() -> None:
    op.drop_column("ad", "ocr_transcript", schema="advertising")
