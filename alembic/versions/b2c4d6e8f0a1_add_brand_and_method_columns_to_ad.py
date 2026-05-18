"""Add predicted_brand, prediction_method to ad

Revision ID: b2c4d6e8f0a1
Revises: 7b1b6e4566fb
Create Date: 2026-05-04

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "b2c4d6e8f0a1"
down_revision: Union[str, None] = "7b1b6e4566fb"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "ad",
        sa.Column("predicted_brand", sa.String(), nullable=True),
        schema="advertising",
    )
    op.add_column(
        "ad",
        sa.Column("prediction_method", sa.String(), nullable=True),
        schema="advertising",
    )


def downgrade() -> None:
    op.drop_column("ad", "predicted_brand", schema="advertising")
    op.drop_column("ad", "prediction_method", schema="advertising")
