"""add subject_id to pulsar tables

Revision ID: e1f2a3b4c5d6
Revises: a7c3f1e9b2d4
Create Date: 2026-07-06 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "e1f2a3b4c5d6"
down_revision: Union[str, None] = "a7c3f1e9b2d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "pulsar_searches",
        sa.Column("subject_id", sa.String(), sa.ForeignKey("subjects.subject_id"), nullable=True),
    )
    op.add_column(
        "pulsar_themes",
        sa.Column("subject_id", sa.String(), sa.ForeignKey("subjects.subject_id"), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("pulsar_themes", "subject_id")
    op.drop_column("pulsar_searches", "subject_id")
