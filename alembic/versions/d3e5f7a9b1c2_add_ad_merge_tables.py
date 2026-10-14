"""Add ad_merge and ad_merge_occurrence tables (journal of the merges of split ads)

Revision ID: d3e5f7a9b1c2
Revises: e3f1a7c9b2d4
Create Date: 2026-10-14

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d3e5f7a9b1c2"
down_revision: Union[str, None] = "e3f1a7c9b2d4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ad_merge",
        sa.Column("merged_ad_id", sa.Text(), nullable=False),
        sa.Column("source_ad_ids", sa.ARRAY(sa.Text()), nullable=False),
        sa.Column("method", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("merged_ad_id"),
        schema="advertising",
    )
    op.create_table(
        "ad_merge_occurrence",
        sa.Column("merged_occurrence_id", sa.Text(), nullable=False),
        sa.Column("source_occurrence_id", sa.Text(), nullable=False),
        sa.Column("merged_ad_id", sa.Text(), nullable=False),
        sa.Column("source_ad_id", sa.Text(), nullable=True),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("position", sa.Integer(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("reverted_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("merged_occurrence_id", "source_occurrence_id"),
        schema="advertising",
    )
    op.create_index(
        "ix_ad_merge_occurrence_merged_ad_id",
        "ad_merge_occurrence",
        ["merged_ad_id"],
        unique=False,
        schema="advertising",
    )
    op.create_index(
        "ix_ad_merge_occurrence_source_occurrence_id",
        "ad_merge_occurrence",
        ["source_occurrence_id"],
        unique=False,
        schema="advertising",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ad_merge_occurrence_source_occurrence_id",
        table_name="ad_merge_occurrence",
        schema="advertising",
    )
    op.drop_index(
        "ix_ad_merge_occurrence_merged_ad_id",
        table_name="ad_merge_occurrence",
        schema="advertising",
    )
    op.drop_table("ad_merge_occurrence", schema="advertising")
    op.drop_table("ad_merge", schema="advertising")
