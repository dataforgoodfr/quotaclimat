"""add_climate_solutions_and_hrfp_columns

Add climate solutions columns (no_hrfp) and mirror all keyword columns for HRFP keywords.
Also add all_keywords JSON column with full keyword metadata.

Revision ID: 8510c148389a
Revises: a8f3c9b2d1e5
Create Date: 2025-12-15 15:48:43.576478

"""
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8510c148389a"
down_revision: Union[str, None] = "a8f3c9b2d1e5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ========================================
    # 0. Create Dictionary and Keyword_Macro_Category tables (with checkfirst behavior)
    # ========================================
    # Note: These tables are shared with the mediatree job. We use try/except to handle
    # the case where they already exist (created by mediatree migrations).
    
    from sqlalchemy import inspect
    
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = inspector.get_table_names()
    
    # Create Dictionary table only if it doesn't exist
    if "dictionary" not in existing_tables:
        op.create_table(
            "dictionary",
            sa.Column("keyword", sa.String(), nullable=False),
            sa.Column("high_risk_of_false_positive", sa.Boolean(), nullable=True, default=True),
            sa.Column("category", sa.String(), nullable=False),
            sa.Column("theme", sa.String(), nullable=False),
            sa.Column("language", sa.String(), nullable=False),
            sa.PrimaryKeyConstraint(
                "keyword",
                "language",
                "category",
                "theme",
                name="pk_keyword_language_category_theme",
            ),
        )
    
    # Create Keyword_Macro_Category table only if it doesn't exist
    if "keyword_macro_category" not in existing_tables:
        op.create_table(
            "keyword_macro_category",
            sa.Column("keyword", sa.String(), nullable=False),
            sa.Column("is_empty", sa.Boolean(), nullable=True, default=False),
            sa.Column("general", sa.Boolean(), nullable=True, default=False),
            sa.Column("agriculture", sa.Boolean(), nullable=True, default=False),
            sa.Column("transport", sa.Boolean(), nullable=True, default=False),
            sa.Column("batiments", sa.Boolean(), nullable=True, default=False),
            sa.Column("energie", sa.Boolean(), nullable=True, default=False),
            sa.Column("industrie", sa.Boolean(), nullable=True, default=False),
            sa.Column("eau", sa.Boolean(), nullable=True, default=False),
            sa.Column("ecosysteme", sa.Boolean(), nullable=True, default=False),
            sa.Column("economie_ressources", sa.Boolean(), nullable=True, default=False),
            sa.PrimaryKeyConstraint("keyword"),
        )

    # ========================================
    # 1. Add climate solutions columns (no_hrfp)
    # ========================================
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_changement_climatique_solutions_no_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("changement_climatique_solutions_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )

    # ========================================
    # 2. Add all HRFP keyword count columns
    # ========================================
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_changement_climatique_constat_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_changement_climatique_causes_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_changement_climatique_consequences_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_attenuation_climatique_solutions_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_adaptation_climatique_solutions_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_changement_climatique_solutions_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_ressources_constat_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_ressources_solutions_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_concepts_generaux_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_causes_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_consequences_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_solutions_hrfp", sa.Integer(), nullable=True),
    )

    # ========================================
    # 3. Add aggregated HRFP count columns by crisis type
    # ========================================
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_climat_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_ressources_hrfp", sa.Integer(), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("number_of_biodiversite_hrfp", sa.Integer(), nullable=True),
    )

    # ========================================
    # 4. Add all HRFP keyword list columns
    # ========================================
    op.add_column(
        "factiva_articles",
        sa.Column("changement_climatique_constat_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("changement_climatique_causes_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("changement_climatique_consequences_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("attenuation_climatique_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("adaptation_climatique_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("changement_climatique_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("ressources_constat_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("ressources_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("biodiversite_concepts_generaux_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("biodiversite_causes_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("biodiversite_consequences_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )
    op.add_column(
        "factiva_articles",
        sa.Column("biodiversite_solutions_keywords_hrfp", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )

    # ========================================
    # 5. Add all_keywords column with full metadata
    # ========================================
    op.add_column(
        "factiva_articles",
        sa.Column("all_keywords", postgresql.JSON(astext_type=sa.Text()), nullable=True),
    )


def downgrade() -> None:
    # Remove all_keywords column from factiva_articles
    op.drop_column("factiva_articles", "all_keywords")

    # Remove HRFP keyword list columns
    op.drop_column("factiva_articles", "biodiversite_solutions_keywords_hrfp")
    op.drop_column("factiva_articles", "biodiversite_consequences_keywords_hrfp")
    op.drop_column("factiva_articles", "biodiversite_causes_keywords_hrfp")
    op.drop_column("factiva_articles", "biodiversite_concepts_generaux_keywords_hrfp")
    op.drop_column("factiva_articles", "ressources_solutions_keywords_hrfp")
    op.drop_column("factiva_articles", "ressources_constat_keywords_hrfp")
    op.drop_column("factiva_articles", "changement_climatique_solutions_keywords_hrfp")
    op.drop_column("factiva_articles", "adaptation_climatique_solutions_keywords_hrfp")
    op.drop_column("factiva_articles", "attenuation_climatique_solutions_keywords_hrfp")
    op.drop_column("factiva_articles", "changement_climatique_consequences_keywords_hrfp")
    op.drop_column("factiva_articles", "changement_climatique_causes_keywords_hrfp")
    op.drop_column("factiva_articles", "changement_climatique_constat_keywords_hrfp")

    # Remove aggregated HRFP count columns
    op.drop_column("factiva_articles", "number_of_biodiversite_hrfp")
    op.drop_column("factiva_articles", "number_of_ressources_hrfp")
    op.drop_column("factiva_articles", "number_of_climat_hrfp")

    # Remove HRFP keyword count columns
    op.drop_column("factiva_articles", "number_of_biodiversite_solutions_hrfp")
    op.drop_column("factiva_articles", "number_of_biodiversite_consequences_hrfp")
    op.drop_column("factiva_articles", "number_of_biodiversite_causes_hrfp")
    op.drop_column("factiva_articles", "number_of_biodiversite_concepts_generaux_hrfp")
    op.drop_column("factiva_articles", "number_of_ressources_solutions_hrfp")
    op.drop_column("factiva_articles", "number_of_ressources_constat_hrfp")
    op.drop_column("factiva_articles", "number_of_changement_climatique_solutions_hrfp")
    op.drop_column("factiva_articles", "number_of_adaptation_climatique_solutions_hrfp")
    op.drop_column("factiva_articles", "number_of_attenuation_climatique_solutions_hrfp")
    op.drop_column("factiva_articles", "number_of_changement_climatique_consequences_hrfp")
    op.drop_column("factiva_articles", "number_of_changement_climatique_causes_hrfp")
    op.drop_column("factiva_articles", "number_of_changement_climatique_constat_hrfp")

    # Remove climate solutions columns (no_hrfp)
    op.drop_column("factiva_articles", "changement_climatique_solutions_keywords")
    op.drop_column("factiva_articles", "number_of_changement_climatique_solutions_no_hrfp")
    
    # Drop Dictionary and Keyword_Macro_Category tables only if they exist
    from sqlalchemy import inspect
    
    bind = op.get_bind()
    inspector = inspect(bind)
    existing_tables = inspector.get_table_names()
    
    # Only drop if tables exist (optional - comment out if you want to keep them)
    if "keyword_macro_category" in existing_tables:
        op.drop_table("keyword_macro_category")
    if "dictionary" in existing_tables:
        op.drop_table("dictionary")


