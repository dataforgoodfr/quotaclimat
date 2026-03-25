"""add id and language for macro category

Revision ID: 5833799c8abc
Revises: previous_revision_id
Create Date: 2026-03-25
"""

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "5833799c8abc"
down_revision = "a33cfeb5e139"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "keyword_macro_category",
        sa.Column("keyword_id", sa.String(), nullable=True),
    )
    op.add_column(
        "keyword_macro_category",
        sa.Column("language", sa.String(), nullable=True),
    )
    # enable uuid generation
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")

    # backfill UUIDs
    op.execute("""
        UPDATE keyword_macro_category
        SET keyword_id = gen_random_uuid()::text
        WHERE keyword_id IS NULL
    """)
    op.alter_column(
        "keyword_macro_category",
        "keyword_id",
        existing_type=sa.String(),
        nullable=False,
    )
    op.drop_constraint(
        "keyword_macro_category_pkey",
        "keyword_macro_category",
        type_="primary"
    )
    op.create_primary_key(
        "pk_keyword_macro_category",
        "keyword_macro_category",
        ["keyword_id"]
    )


def downgrade():
    op.drop_constraint(
        "pk_keyword_macro_category",
        "keyword_macro_category",
        type_="primary"
    )
    op.create_primary_key(
        "keyword_macro_category_pkey",
        "keyword_macro_category",
        ["keyword"]
    )
    op.drop_column("keyword_macro_category", "language")
    op.drop_column("keyword_macro_category", "keyword_id")