"""initial dictionary, stop_word, keyword_macro_category tables

dictionary and keyword_macro_category are first touched by later
migrations (44f13b7eebd4, 5833799c8abc) with ALTER-only statements, and
stop_word is never referenced by any migration at all - all three were
only ever created via Base.metadata.create_all(). This revision creates
them as they existed right before 44f13b7eebd4 (the first ALTER on
dictionary), reconstructed from git history of
postgres/schemas/models.py:
- dictionary / keyword_macro_category: commit 676dbdf4714c
  (2025-07-02, "Feat/macro category").
- stop_word: never altered by any migration, so created here with its
  current full column set.

Revision ID: c14476f987a0
Revises: 827fb6dde3bb
Create Date: 2026-10-07 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = 'c14476f987a0'
down_revision: Union[str, None] = '827fb6dde3bb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'dictionary',
        sa.Column('keyword', sa.String(), nullable=False),
        sa.Column('language', sa.String(), nullable=False),
        sa.Column('high_risk_of_false_positive', sa.Boolean(), nullable=True),
        sa.Column('solution', sa.Boolean(), nullable=True),
        sa.Column('consequence', sa.Boolean(), nullable=True),
        sa.Column('cause', sa.Boolean(), nullable=True),
        sa.Column('general_concepts', sa.Boolean(), nullable=True),
        sa.Column('statement', sa.Boolean(), nullable=True),
        sa.Column('crisis_climate', sa.Boolean(), nullable=True),
        sa.Column('crisis_biodiversity', sa.Boolean(), nullable=True),
        sa.Column('crisis_resource', sa.Boolean(), nullable=True),
        sa.Column('categories', postgresql.ARRAY(sa.String()), nullable=True),
        sa.Column('themes', postgresql.ARRAY(sa.String()), nullable=True),
        sa.PrimaryKeyConstraint('keyword', 'language', name='pk_keyword_language'),
        if_not_exists=True,
    )

    op.create_table(
        'keyword_macro_category',
        sa.Column('keyword', sa.String(), nullable=False),
        sa.Column('is_empty', sa.Boolean(), nullable=True),
        sa.Column('general', sa.Boolean(), nullable=True),
        sa.Column('agriculture', sa.Boolean(), nullable=True),
        sa.Column('transport', sa.Boolean(), nullable=True),
        sa.Column('batiments', sa.Boolean(), nullable=True),
        sa.Column('energie', sa.Boolean(), nullable=True),
        sa.Column('industrie', sa.Boolean(), nullable=True),
        sa.Column('eau', sa.Boolean(), nullable=True),
        sa.Column('ecosysteme', sa.Boolean(), nullable=True),
        sa.Column('economie_ressources', sa.Boolean(), nullable=True),
        sa.PrimaryKeyConstraint('keyword', name='keyword_macro_category_pkey'),
        if_not_exists=True,
    )

    op.create_table(
        'stop_word',
        sa.Column('id', sa.Text(), nullable=False),
        sa.Column('keyword_id', sa.Text(), nullable=True),
        sa.Column('channel_title', sa.String(), nullable=True),
        sa.Column('context', sa.String(), nullable=False),
        sa.Column('count', sa.Integer(), nullable=True),
        sa.Column('keyword', sa.String(), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text("(now() at time zone 'utc')"),
            nullable=True,
        ),
        sa.Column('start_date', sa.DateTime(timezone=True), nullable=True),
        sa.Column('updated_at', sa.DateTime(), nullable=True),
        sa.Column('validated', sa.Boolean(), nullable=True),
        sa.Column('country', sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        if_not_exists=True,
    )


def downgrade() -> None:
    op.drop_table('stop_word')
    op.drop_table('keyword_macro_category')
    op.drop_table('dictionary')
