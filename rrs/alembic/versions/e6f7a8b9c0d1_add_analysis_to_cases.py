"""add analysis to cases

Revision ID: e6f7a8b9c0d1
Revises: d5e9f0a1b2c3
Create Date: 2026-08-07

"""
from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "e6f7a8b9c0d1"
down_revision: Union[str, None] = "d5e9f0a1b2c3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("cases", sa.Column("analysis", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("cases", "analysis")
