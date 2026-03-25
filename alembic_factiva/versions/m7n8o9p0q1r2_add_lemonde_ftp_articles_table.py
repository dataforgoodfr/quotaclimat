"""Add lemonde_ftp_articles table for Le Monde FTP ingestion

Revision ID: m7n8o9p0q1r2
Revises: l6m7n8o9p0q1
Create Date: 2026-02-23 14:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "m7n8o9p0q1r2"
down_revision: Union[str, None] = "l6m7n8o9p0q1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "lemonde_ftp_articles",
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
        # Keyword counts - non HRFP (high risk of false positive) - UNIQUE keywords only
        sa.Column("number_of_changement_climatique_constat_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_changement_climatique_causes_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_changement_climatique_consequences_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_attenuation_climatique_solutions_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_adaptation_climatique_solutions_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_changement_climatique_solutions_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_ressources_constat_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_ressources_solutions_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_concepts_generaux_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_causes_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_consequences_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_solutions_no_hrfp", sa.Integer(), nullable=True),
        # Keyword counts - HRFP (high risk of false positive) - UNIQUE keywords only
        sa.Column("number_of_changement_climatique_constat_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_changement_climatique_causes_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_changement_climatique_consequences_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_attenuation_climatique_solutions_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_adaptation_climatique_solutions_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_changement_climatique_solutions_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_ressources_constat_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_ressources_solutions_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_concepts_generaux_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_causes_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_consequences_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_solutions_hrfp", sa.Integer(), nullable=True),
        # Aggregated counts by crisis type - non HRFP (sum of causal links)
        sa.Column("number_of_climat_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_ressources_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_no_hrfp", sa.Integer(), nullable=True),
        # Aggregated counts by crisis type - HRFP (sum of causal links)
        sa.Column("number_of_climat_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_ressources_hrfp", sa.Integer(), nullable=True),
        sa.Column("number_of_biodiversite_hrfp", sa.Integer(), nullable=True),
        # Aggregated counts for ALL crises combined - non HRFP (unique keywords across all crises)
        sa.Column("number_of_crises_no_hrfp", sa.Integer(), nullable=True),
        sa.Column("crises_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        # Aggregated counts for ALL crises combined - HRFP (unique keywords across all crises)
        sa.Column("number_of_crises_hrfp", sa.Integer(), nullable=True),
        sa.Column("crises_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        # Keyword lists by causal link - non HRFP - JSON arrays with ALL occurrences (including duplicates)
        sa.Column("changement_climatique_constat_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("changement_climatique_causes_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("changement_climatique_consequences_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("attenuation_climatique_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("adaptation_climatique_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("changement_climatique_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("ressources_constat_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("ressources_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("biodiversite_concepts_generaux_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("biodiversite_causes_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("biodiversite_consequences_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("biodiversite_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        # Keyword lists by causal link - HRFP - JSON arrays with ALL occurrences (including duplicates)
        sa.Column("changement_climatique_constat_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("changement_climatique_causes_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("changement_climatique_consequences_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("attenuation_climatique_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("adaptation_climatique_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("changement_climatique_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("ressources_constat_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("ressources_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("biodiversite_concepts_generaux_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("biodiversite_causes_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("biodiversite_consequences_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.Column("biodiversite_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        # All keywords with full metadata (keyword, theme, category, count_keyword, is_hrfp)
        sa.Column("all_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("an"),
    )

    op.create_index(
        "ix_lemonde_ftp_articles_source_code", "lemonde_ftp_articles", ["source_code"]
    )
    op.create_index(
        "ix_lemonde_ftp_articles_is_deleted", "lemonde_ftp_articles", ["is_deleted"]
    )
    op.create_index(
        "ix_lemonde_ftp_articles_publication_datetime",
        "lemonde_ftp_articles",
        ["publication_datetime"],
    )
    op.create_index(
        "ix_lemonde_ftp_articles_updated_at",
        "lemonde_ftp_articles",
        ["updated_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_lemonde_ftp_articles_updated_at", table_name="lemonde_ftp_articles"
    )
    op.drop_index(
        "ix_lemonde_ftp_articles_publication_datetime", table_name="lemonde_ftp_articles"
    )
    op.drop_index("ix_lemonde_ftp_articles_is_deleted", table_name="lemonde_ftp_articles")
    op.drop_index("ix_lemonde_ftp_articles_source_code", table_name="lemonde_ftp_articles")
    op.drop_table("lemonde_ftp_articles")
