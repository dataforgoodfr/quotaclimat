"""add_source_classification_and_crises_columns

Add source_classification table and new crises aggregation columns to factiva_articles.

Revision ID: f9a2b3c4d5e6
Revises: 8510c148389a
Create Date: 2025-12-16 10:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "f9a2b3c4d5e6"
down_revision: Union[str, None] = "8510c148389a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ========================================
    # 1. Create source_classification table
    # ========================================
    op.create_table(
        "source_classification",
        sa.Column("source_type", sa.String(), nullable=False),
        sa.Column("source_name", sa.String(), nullable=False),
        sa.Column("source_code", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("source_code", name="pk_source_classification_source_code"),
    )

    # ========================================
    # 2. Add crises aggregation columns to factiva_articles (non-HRFP)
    # ========================================
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_crises_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("crises_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )

    # ========================================
    # 3. Add crises aggregation columns to factiva_articles (HRFP)
    # ========================================
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_crises_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("crises_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    # Remove crises columns from factiva_articles
    op.drop_column("factiva_articles", "crises_keywords_hrfp")
    op.drop_column("factiva_articles", "number_of_crises_hrfp")
    op.drop_column("factiva_articles", "crises_keywords")
    op.drop_column("factiva_articles", "number_of_crises_no_hrfp")
    
    # Drop source_classification table
    op.drop_table("source_classification")
