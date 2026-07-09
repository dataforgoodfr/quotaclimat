"""add subject_title to subjects

Revision ID: b9d7e2f1a3c8
Revises: e1f2a3b4c5d6
Create Date: 2026-07-09

"""
from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "b9d7e2f1a3c8"
down_revision: Union[str, None] = "e1f2a3b4c5d6"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("subjects", sa.Column("subject_title", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("subjects", "subject_title")
