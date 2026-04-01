"""Rename ad.chunks to ad.chunks_fingerprint

Revision ID: b7e2f1c3a456
Revises: a33cfeb5e139
Create Date: 2026-04-01 12:00:00.000000

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7e2f1c3a456"
down_revision: Union[str, None] = "a33cfeb5e139"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.alter_column(
        "ad",
        "chunks",
        new_column_name="chunks_fingerprint",
        schema="advertising",
    )


def downgrade() -> None:
    op.alter_column(
        "ad",
        "chunks_fingerprint",
        new_column_name="chunks",
        schema="advertising",
    )
