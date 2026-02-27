"""Add factiva_articles table for print media ingestion

Revision ID: d2e89a74b3c1
Revises:
Create Date: 2025-10-22 10:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d2e89a74b3c1"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "factiva_articles",
        sa.Column("an", sa.String(), nullable=False),
        sa.Column("document_type", sa.String(), nullable=True),
        sa.Column("action", sa.String(), nullable=True),
        sa.Column("event_type", sa.String(), nullable=True),
        sa.Column("title", sa.Text(), nullable=True),
        sa.Column("body", sa.Text(), nullable=True),
        sa.Column("snippet", sa.Text(), nullable=True),
        sa.Column("art", sa.Text(), nullable=True),
        sa.Column("byline", sa.Text(), nullable=True),
        sa.Column("credit", sa.Text(), nullable=True),
        sa.Column("dateline", sa.Text(), nullable=True),
        sa.Column("source_code", sa.String(), nullable=True),
        sa.Column("source_name", sa.Text(), nullable=True),
        sa.Column("publisher_name", sa.Text(), nullable=True),
        sa.Column("section", sa.Text(), nullable=True),
        sa.Column("copyright", sa.Text(), nullable=True),
        sa.Column("publication_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("publication_datetime", sa.DateTime(timezone=True), nullable=True),
        sa.Column("modification_date", sa.DateTime(timezone=True), nullable=True),
        sa.Column("modification_datetime", sa.DateTime(timezone=True), nullable=True),
        sa.Column("ingestion_datetime", sa.DateTime(timezone=True), nullable=True),
        sa.Column("availability_datetime", sa.DateTime(timezone=True), nullable=True),
        sa.Column("language_code", sa.String(), nullable=True),
        sa.Column("region_of_origin", sa.String(), nullable=True),
        sa.Column("word_count", sa.Integer(), nullable=True),
        sa.Column("company_codes", sa.Text(), nullable=True),
        sa.Column("company_codes_about", sa.Text(), nullable=True),
        sa.Column("company_codes_association", sa.Text(), nullable=True),
        sa.Column("company_codes_lineage", sa.Text(), nullable=True),
        sa.Column("company_codes_occur", sa.Text(), nullable=True),
        sa.Column("company_codes_relevance", sa.Text(), nullable=True),
        sa.Column("subject_codes", sa.Text(), nullable=True),
        sa.Column("region_codes", sa.Text(), nullable=True),
        sa.Column("industry_codes", sa.Text(), nullable=True),
        sa.Column("person_codes", sa.Text(), nullable=True),
        sa.Column("currency_codes", sa.Text(), nullable=True),
        sa.Column("market_index_codes", sa.Text(), nullable=True),
        sa.Column("allow_translation", sa.Boolean(), nullable=True),
        sa.Column("attrib_code", sa.String(), nullable=True),
        sa.Column("authors", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("clusters", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("content_type_codes", sa.Text(), nullable=True),
        sa.Column("footprint_company_codes", sa.Text(), nullable=True),
        sa.Column("footprint_person_codes", sa.Text(), nullable=True),
        sa.Column("industry_classification_benchmark_codes", sa.Text(), nullable=True),
        sa.Column("newswires_codes", sa.Text(), nullable=True),
        sa.Column("org_type_codes", sa.Text(), nullable=True),
        sa.Column("pub_page", sa.String(), nullable=True),
        sa.Column("restrictor_codes", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(now() at time zone 'utc')"),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "is_deleted", sa.Boolean(), nullable=False, server_default=sa.text("false")
        ),
        sa.PrimaryKeyConstraint("an"),
    )

    op.create_index(
        "ix_factiva_articles_source_code", "factiva_articles", ["source_code"]
    )
    op.create_index(
        "ix_factiva_articles_is_deleted", "factiva_articles", ["is_deleted"]
    )
    op.create_index(
        "ix_factiva_articles_publication_datetime",
        "factiva_articles",
        ["publication_datetime"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_factiva_articles_publication_datetime", table_name="factiva_articles"
    )
    op.drop_index("ix_factiva_articles_is_deleted", table_name="factiva_articles")
    op.drop_index("ix_factiva_articles_source_code", table_name="factiva_articles")
    op.drop_table("factiva_articles")
