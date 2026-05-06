"""Add useful indexes on advertising tables

Revision ID: 7b1b6e4566fb
Revises: 901a1e430bf5
Create Date: 2026-04-28 14:40:29.496558

"""

from typing import Sequence, Union

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "7b1b6e4566fb"
down_revision: Union[str, None] = "901a1e430bf5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_index(
        "ix_ad_first_detection_date",
        "ad",
        ["first_detection_date"],
        unique=False,
        schema="advertising",
    )
    op.create_index(
        "ix_ad_occurrence_ad_id",
        "ad_occurrence",
        ["ad_id"],
        unique=False,
        schema="advertising",
        if_not_exists=True,
    )
    op.create_index(
        "ix_ad_occurrence_occurrence_date",
        "ad_occurrence",
        ["occurrence_date"],
        unique=False,
        schema="advertising",
    )


def downgrade() -> None:
    op.drop_index(
        "ix_ad_occurrence_occurrence_date",
        table_name="ad_occurrence",
        schema="advertising",
    )
    op.drop_index(
        "ix_ad_occurrence_ad_id", table_name="ad_occurrence", schema="advertising"
    )
    op.drop_index("ix_ad_first_detection_date", table_name="ad", schema="advertising")
