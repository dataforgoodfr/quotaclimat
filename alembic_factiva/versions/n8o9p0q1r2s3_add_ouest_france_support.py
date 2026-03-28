"""Add OuestFrance support: article_url/tags columns + OUESTFR source

Adds optional article_url and tags columns to factiva_articles and
lemonde_ftp_articles tables for non-Factiva sources. Inserts OUESTFR
into source_classification.

Revision ID: n8o9p0q1r2s3
Revises: m7n8o9p0q1r2
Create Date: 2026-03-28 10:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "n8o9p0q1r2s3"
down_revision: Union[str, None] = "m7n8o9p0q1r2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add article_url and tags columns to factiva_articles
    op.add_column(
        "factiva_articles",
        sa.Column("article_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("tags", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )

    # Add article_url and tags columns to lemonde_ftp_articles
    op.add_column(
        "lemonde_ftp_articles",
        sa.Column("article_url", sa.Text(), nullable=True),
    )
    op.add_column(
        "lemonde_ftp_articles",
        sa.Column("tags", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )

    # Insert OUESTFR into source_classification
    op.execute(
        sa.text(
            """
            INSERT INTO source_classification (source_type, source_name, source_code, source_owner, media_all, source_region)
            VALUES ('Presse Régionale', 'Ouest-France', 'OUESTFR', 'SIPA Ouest-France', 'Ouest-France', 'Bretagne')
            ON CONFLICT (source_code) DO NOTHING
            """
        )
    )


def downgrade() -> None:
    # Remove OUESTFR from source_classification
    op.execute(
        sa.text(
            "DELETE FROM source_classification WHERE source_code = 'OUESTFR'"
        )
    )

    # Remove columns from lemonde_ftp_articles
    op.drop_column("lemonde_ftp_articles", "tags")
    op.drop_column("lemonde_ftp_articles", "article_url")

    # Remove columns from factiva_articles
    op.drop_column("factiva_articles", "tags")
    op.drop_column("factiva_articles", "article_url")
