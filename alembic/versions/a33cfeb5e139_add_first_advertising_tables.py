"""Add first advertising tables

Revision ID: a33cfeb5e139
Revises: a578d21d7aee
Create Date: 2026-03-24 19:34:43.158153

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a33cfeb5e139"
down_revision: Union[str, None] = "a578d21d7aee"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("create schema advertising")
    op.create_table(
        "ad",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("first_detection_date", sa.DateTime(), nullable=False),
        sa.Column("duration_sec", sa.Double(), nullable=False),
        sa.Column("chunks", sa.JSON(), nullable=False),
        sa.Column("fragment_type", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        schema="advertising",
    )
    op.create_table(
        "ad_occurrence",
        sa.Column("id", sa.Text(), nullable=False),
        sa.Column("deleted_at", sa.DateTime(), nullable=True),
        sa.Column("occurrence_date", sa.DateTime(), nullable=False),
        sa.Column("channel_name", sa.String(), nullable=False),
        sa.Column("ad_id", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["ad_id"],
            ["advertising.ad.id"],
        ),
        sa.PrimaryKeyConstraint("id"),
        schema="advertising",
    )


def downgrade() -> None:
    op.drop_table("ad_occurrence", schema="advertising")
    op.drop_table("ad", schema="advertising")
    op.execute("drop schema advertising")
