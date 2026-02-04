"""add_sector_columns

Add sector keyword and prediction columns to factiva_articles table.

Revision ID: l6m7n8o9p0q1
Revises: k5e6f7g8h9i1
Create Date: 2026-02-04 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSON

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "l6m7n8o9p0q1"
down_revision: Union[str, None] = "k5e6f7g8h9i1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add sector keyword list columns (JSON arrays of unique keywords)
    op.add_column(
        "factiva_articles",
        sa.Column("agriculture_alimentation_unique_keywords", JSON, nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("mobilite_unique_keywords", JSON, nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("batiments_amenagement_unique_keywords", JSON, nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("economie_circulaire_unique_keywords", JSON, nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("energie_unique_keywords", JSON, nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("industrie_unique_keywords", JSON, nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("eau_unique_keywords", JSON, nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("ecosysteme_unique_keywords", JSON, nullable=True),
    )
    
    # Add sector keyword count columns (number of unique keywords)
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_agriculture_alimentation_keywords", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_mobilite_keywords", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_batiments_amenagement_keywords", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_economie_circulaire_keywords", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_energie_keywords", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_industrie_keywords", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_eau_keywords", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_ecosysteme_keywords", sa.Integer(), nullable=True),
    )
    
    # Add sector prediction flag columns
    op.add_column(
        "factiva_articles",
        sa.Column("predict_agriculture_alimentation", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_mobilite", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_batiments_amenagement", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_economie_circulaire", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_energie", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_industrie", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_eau", sa.Boolean(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("predict_ecosysteme", sa.Boolean(), nullable=True),
    )


def downgrade() -> None:
    # Remove sector prediction columns
    op.drop_column("factiva_articles", "predict_ecosysteme")
    op.drop_column("factiva_articles", "predict_eau")
    op.drop_column("factiva_articles", "predict_industrie")
    op.drop_column("factiva_articles", "predict_energie")
    op.drop_column("factiva_articles", "predict_economie_circulaire")
    op.drop_column("factiva_articles", "predict_batiments_amenagement")
    op.drop_column("factiva_articles", "predict_mobilite")
    op.drop_column("factiva_articles", "predict_agriculture_alimentation")
    
    # Remove sector keyword count columns
    op.drop_column("factiva_articles", "number_of_ecosysteme_keywords")
    op.drop_column("factiva_articles", "number_of_eau_keywords")
    op.drop_column("factiva_articles", "number_of_industrie_keywords")
    op.drop_column("factiva_articles", "number_of_energie_keywords")
    op.drop_column("factiva_articles", "number_of_economie_circulaire_keywords")
    op.drop_column("factiva_articles", "number_of_batiments_amenagement_keywords")
    op.drop_column("factiva_articles", "number_of_mobilite_keywords")
    op.drop_column("factiva_articles", "number_of_agriculture_alimentation_keywords")
    
    # Remove sector keyword list columns
    op.drop_column("factiva_articles", "ecosysteme_unique_keywords")
    op.drop_column("factiva_articles", "eau_unique_keywords")
    op.drop_column("factiva_articles", "industrie_unique_keywords")
    op.drop_column("factiva_articles", "energie_unique_keywords")
    op.drop_column("factiva_articles", "economie_circulaire_unique_keywords")
    op.drop_column("factiva_articles", "batiments_amenagement_unique_keywords")
    op.drop_column("factiva_articles", "mobilite_unique_keywords")
    op.drop_column("factiva_articles", "agriculture_alimentation_unique_keywords")
