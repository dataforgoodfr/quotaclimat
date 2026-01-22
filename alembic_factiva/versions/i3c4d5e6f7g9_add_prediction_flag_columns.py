"""add_prediction_flag_columns

Add prediction flag columns to factiva_articles table for pre-calculated crisis predictions.

Revision ID: i3c4d5e6f7g9
Revises: h2b3c4d5e6f8
Create Date: 2026-01-22 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "i3c4d5e6f7g9"
down_revision: Union[str, None] = "h2b3c4d5e6f8"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add prediction flag columns to factiva_articles table
    # These flags are pre-calculated based on keyword scores, HRFP multipliers,
    # article length segments, and threshold comparisons
    
    # Global crisis predictions
    op.add_column(
        "factiva_articles",
        sa.Column("predict_at_least_one_crise", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_climat", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_biodiversite", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_ressources", sa.Boolean(), nullable=True),
    )
    
    # Climate causal link predictions
    op.add_column(
        "factiva_articles",
        sa.Column("predict_climat_constat", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_climat_cause", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_climat_consequence", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_climat_solution", sa.Boolean(), nullable=True),
    )
    
    # Biodiversity causal link predictions
    op.add_column(
        "factiva_articles",
        sa.Column("predict_biodiversite_constat", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_biodiversite_cause", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_biodiversite_consequence", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_biodiversite_solution", sa.Boolean(), nullable=True),
    )
    
    # Resource causal link predictions
    op.add_column(
        "factiva_articles",
        sa.Column("predict_ressources_constat", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_ressources_solution", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    # Remove all prediction flag columns
    op.drop_column("factiva_articles", "predict_ressources_solution")
    op.drop_column("factiva_articles", "predict_ressources_constat")
    op.drop_column("factiva_articles", "predict_biodiversite_solution")
    op.drop_column("factiva_articles", "predict_biodiversite_consequence")
    op.drop_column("factiva_articles", "predict_biodiversite_cause")
    op.drop_column("factiva_articles", "predict_biodiversite_constat")
    op.drop_column("factiva_articles", "predict_climat_solution")
    op.drop_column("factiva_articles", "predict_climat_consequence")
    op.drop_column("factiva_articles", "predict_climat_cause")
    op.drop_column("factiva_articles", "predict_climat_constat")
    op.drop_column("factiva_articles", "predict_ressources")
    op.drop_column("factiva_articles", "predict_biodiversite")
    op.drop_column("factiva_articles", "predict_climat")
    op.drop_column("factiva_articles", "predict_at_least_one_crise")
