"""Add keyword columns to factiva_articles and create stats_factiva_articles table

Revision ID: a8f3c9b2d1e5
Revises: d2e89a74b3c1
Create Date: 2025-11-14 10:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a8f3c9b2d1e5"
down_revision: Union[str, None] = "d2e89a74b3c1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add keyword count columns to factiva_articles table (unique keywords only)
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_changement_climatique_constat_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_changement_climatique_causes_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_changement_climatique_consequences_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_attenuation_climatique_solutions_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_adaptation_climatique_solutions_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_ressources_constat_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_ressources_solutions_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_concepts_generaux_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_causes_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_consequences_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_solutions_no_hrfp", sa.Integer(), nullable=True),
    )

    # Add aggregated count columns by crisis type
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_climat_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_ressources_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_no_hrfp", sa.Integer(), nullable=True),
    )

    # Add keyword list columns (JSON arrays with all occurrences including duplicates)
    op.add_column(
        "factiva_articles",
        sa.Column("changement_climatique_constat_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("changement_climatique_causes_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("changement_climatique_consequences_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("attenuation_climatique_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("adaptation_climatique_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("ressources_constat_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("ressources_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("biodiversite_concepts_generaux_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("biodiversite_causes_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("biodiversite_consequences_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("biodiversite_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )

    # Create stats_factiva_articles table
    op.create_table(
        "stats_factiva_articles",
        sa.Column("source_code", sa.String(), nullable=False),
        sa.Column("publication_datetime", sa.DateTime(timezone=True), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("(now() at time zone 'utc')"),
            nullable=True,
        ),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("source_code", "publication_datetime"),
    )

    # Create indexes for stats_factiva_articles
    op.create_index(
        "ix_stats_factiva_articles_source_code",
        "stats_factiva_articles",
        ["source_code"],
    )
    op.create_index(
        "ix_stats_factiva_articles_publication_datetime",
        "stats_factiva_articles",
        ["publication_datetime"],
    )


def downgrade() -> None:
    # Drop stats_factiva_articles table and indexes
    op.drop_index(
        "ix_stats_factiva_articles_publication_datetime",
        table_name="stats_factiva_articles",
    )
    op.drop_index(
        "ix_stats_factiva_articles_source_code", table_name="stats_factiva_articles"
    )
    op.drop_table("stats_factiva_articles")

    # Remove keyword list columns from factiva_articles
    op.drop_column("factiva_articles", "biodiversite_solutions_keywords")
    op.drop_column("factiva_articles", "biodiversite_consequences_keywords")
    op.drop_column("factiva_articles", "biodiversite_causes_keywords")
    op.drop_column("factiva_articles", "biodiversite_concepts_generaux_keywords")
    op.drop_column("factiva_articles", "ressources_solutions_keywords")
    op.drop_column("factiva_articles", "ressources_constat_keywords")
    op.drop_column("factiva_articles", "adaptation_climatique_solutions_keywords")
    op.drop_column("factiva_articles", "attenuation_climatique_solutions_keywords")
    op.drop_column("factiva_articles", "changement_climatique_consequences_keywords")
    op.drop_column("factiva_articles", "changement_climatique_causes_keywords")
    op.drop_column("factiva_articles", "changement_climatique_constat_keywords")

    # Remove aggregated count columns from factiva_articles
    op.drop_column("factiva_articles", "number_of_biodiversite_no_hrfp")
    op.drop_column("factiva_articles", "number_of_ressources_no_hrfp")
    op.drop_column("factiva_articles", "number_of_climat_no_hrfp")

    # Remove keyword count columns from factiva_articles
    op.drop_column("factiva_articles", "number_of_biodiversite_solutions_no_hrfp")
    op.drop_column("factiva_articles", "number_of_biodiversite_consequences_no_hrfp")
    op.drop_column("factiva_articles", "number_of_biodiversite_causes_no_hrfp")
    op.drop_column("factiva_articles", "number_of_biodiversite_concepts_generaux_no_hrfp")
    op.drop_column("factiva_articles", "number_of_ressources_solutions_no_hrfp")
    op.drop_column("factiva_articles", "number_of_ressources_constat_no_hrfp")
    op.drop_column("factiva_articles", "number_of_adaptation_climatique_solutions_no_hrfp")
    op.drop_column("factiva_articles", "number_of_attenuation_climatique_solutions_no_hrfp")
    op.drop_column("factiva_articles", "number_of_changement_climatique_consequences_no_hrfp")
    op.drop_column("factiva_articles", "number_of_changement_climatique_causes_no_hrfp")
    op.drop_column("factiva_articles", "number_of_changement_climatique_constat_no_hrfp")

