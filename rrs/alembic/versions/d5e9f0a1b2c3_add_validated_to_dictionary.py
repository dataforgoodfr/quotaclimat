"""add validated to dictionary

Revision ID: d5e9f0a1b2c3
Revises: c4e6f8a2b1d9
Create Date: 2026-07-27

"""
from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "d5e9f0a1b2c3"
down_revision: Union[str, None] = "c4e6f8a2b1d9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "dictionary", sa.Column("validated", sa.Boolean(), nullable=True)
    )
    op.execute("UPDATE dictionary SET validated = true WHERE validated IS NULL")


def downgrade() -> None:
    op.drop_column("dictionary", "validated")
