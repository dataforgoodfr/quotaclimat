"""Add ad_screenshot table (screenshots of the ads and occurrences, for visual checks)

Revision ID: f4a8c2e6d1b3
Revises: 58e7bbf8f59c
Create Date: 2026-10-24

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "f4a8c2e6d1b3"
down_revision: Union[str, None] = "58e7bbf8f59c"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "ad_screenshot",
        sa.Column("target_type", sa.String(), nullable=False),
        sa.Column("target_id", sa.Text(), nullable=False),
        sa.Column("ad_id", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=True),
        sa.Column("image_url", sa.Text(), nullable=True),
        sa.Column("frame_offsets_sec", sa.ARRAY(sa.Double()), nullable=True),
        sa.Column("nb_frames", sa.Integer(), nullable=True),
        sa.Column("details", sa.JSON(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(), server_default=sa.func.now(), nullable=False
        ),
        sa.PrimaryKeyConstraint("target_type", "target_id"),
        schema="advertising",
    )
    op.create_index(
        "ix_ad_screenshot_ad_id",
        "ad_screenshot",
        ["ad_id"],
        unique=False,
        schema="advertising",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ad_screenshot_ad_id", table_name="ad_screenshot", schema="advertising"
    )
    op.drop_table("ad_screenshot", schema="advertising")
