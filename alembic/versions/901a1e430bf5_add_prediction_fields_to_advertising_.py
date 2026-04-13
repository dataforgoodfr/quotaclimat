"""Add prediction fields to advertising models

Revision ID: 901a1e430bf5
Revises: 5833799c8abc
Create Date: 2026-04-13 15:54:35.288724

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "901a1e430bf5"
down_revision: Union[str, None] = "5833799c8abc"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "ad", sa.Column("transcript", sa.Text(), nullable=True), schema="advertising"
    )
    op.add_column(
        "ad", sa.Column("prediction", sa.JSON(), nullable=True), schema="advertising"
    )
    op.add_column(
        "ad",
        sa.Column("prediction_status", sa.String(), nullable=True),
        schema="advertising",
    )
    op.add_column(
        "ad",
        sa.Column("prediction_confidence", sa.Double(), nullable=True),
        schema="advertising",
    )
    op.add_column(
        "ad",
        sa.Column("predicted_sector", sa.String(), nullable=True),
        schema="advertising",
    )
    op.add_column(
        "ad",
        sa.Column("predicted_product_category", sa.String(), nullable=True),
        schema="advertising",
    )


def downgrade() -> None:
    op.drop_column("ad", "predicted_product_category", schema="advertising")
    op.drop_column("ad", "predicted_sector", schema="advertising")
    op.drop_column("ad", "prediction_confidence", schema="advertising")
    op.drop_column("ad", "prediction_status", schema="advertising")
    op.drop_column("ad", "prediction", schema="advertising")
    op.drop_column("ad", "transcript", schema="advertising")
