"""Add obsolete_at and replaced_by_ad_id to advertising.ad (ads merged by hand, kept for their history)

Revision ID: a7c3e9f1b2d4
Revises: f4a8c2e6d1b3
Create Date: 2026-11-12

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "a7c3e9f1b2d4"
down_revision: Union[str, None] = "f4a8c2e6d1b3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "ad",
        sa.Column("obsolete_at", sa.DateTime(), nullable=True),
        schema="advertising",
    )
    op.add_column(
        "ad",
        sa.Column("replaced_by_ad_id", sa.Text(), nullable=True),
        schema="advertising",
    )


def downgrade() -> None:
    op.drop_column("ad", "replaced_by_ad_id", schema="advertising")
    op.drop_column("ad", "obsolete_at", schema="advertising")
